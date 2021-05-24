(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure.stacktrace :as trace]
            [clojure.core :as core]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [jepsen.util :as util]
            [jepsen.store :as store]
            [jepsen.checker.perf :as perf]
            [multiset.core :as multiset]
            [gnuplot.core :as g]
            [knossos [core :as knossos]
                     [history :as history]
                     [model :as model]]))

(defprotocol Checker
  (check [checker test model history]
         "Verify the history is correct. Returns a map like

         {:valid? true}

         or

         {:valid?    false
          :failed-at [details of specific operations]}

         and maybe there can be some stats about what fraction of requests
         were corrupt, etc."))

(defn check-safe
  "Like check, but wraps exceptions up and returns them as a map like

  {:valid? nil :error \"...\"}"
  [checker test model history]
  (try (check checker test model history)
       (catch Throwable t
         {:valid? false
          :error (with-out-str (trace/print-cause-trace t))})))

(def unbridled-optimism
  "Everything is awesoooommmmme!"
  (reify Checker
    (check [this test model history] {:valid? true})))

(def linearizable
  "Validates linearizability with Knossos."
  (reify Checker
    (check [this test model history]
      (knossos/analysis model history))))

(def queue
  "Every dequeue must come from somewhere. Validates queue operations by
  assuming every non-failing enqueue succeeded, and only OK dequeues succeeded,
  then reducing the model with that history. Every subhistory of every queue
  should obey this property. Should probably be used with an unordered queue
  model, because we don't look for alternate orderings. O(n)."
  (reify Checker
    (check [this test model history]
      (let [final (->> history
                       (r/filter (fn select [op]
                                   (condp = (:f op)
                                     :enqueue (knossos/invoke? op)
                                     :dequeue (knossos/ok? op)
                                     false)))
                                 (reduce model/step model))]
        (if (model/inconsistent? final)
          {:valid? false
           :error  (:msg final)}
          {:valid?      true
           :final-queue final})))))

(def set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter knossos/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"})

        (let [; The OK set is every read value which we tried to add
              ok          (set/intersection final-read attempts)

              ; Unexpected records are those we *never* attempted.
              unexpected  (set/difference final-read attempts)

              ; Lost records are those we definitely added but weren't read
              lost        (set/difference adds final-read)

              ; Recovered records are those where we didn't know if the add
              ; succeeded or not, but we found them in the final set.
              recovered   (set/difference ok adds)]

          {:valid?          (and (empty? lost) (empty? unexpected))
           :ok              (util/integer-interval-set-str ok)
           :lost            (util/integer-interval-set-str lost)
           :unexpected      (util/integer-interval-set-str unexpected)
           :recovered       (util/integer-interval-set-str recovered)
           :ok-frac         (util/fraction (count ok) (count attempts))
           :unexpected-frac (util/fraction (count unexpected) (count attempts))
           :lost-frac       (util/fraction (count lost) (count attempts))
           :recovered-frac  (util/fraction (count recovered) (count attempts))})))))

(def associative-map
  "Given a set of :assoc operations interspersed with :read's, verifies that
  the newest assoc'ed value for each key is present in each read, and that :read's
  contain only key-value pairs for which an assoc was attempted. The map should have stabilized
  before a :read is issued, such that all :invoke's have been :ok'ed, :info'ed or :fail'ed. In that way,
  map is more like a multi-phase set model than the counter model."
  (reify Checker
    (check [this test model history]
      (loop [history (seq (history/complete history))
             reads []
             possible {}
             confirmed {}]
        (if (nil? history)
          (let [errors (remove (fn [{:keys [confirmed possible actual]}]
                                 (and (every? (fn [[k v]]
                                                (or (= v (get confirmed k))
                                                    (some #{v} (get possible k)))) actual)
                                      (every? (clojure.core/set (keys actual)) (keys confirmed))))
                               reads)]
            {:valid? (and (seq reads) (empty? errors))
             :reads reads
             :errors errors})
          (let [op (first history)
                history (next history)]
            (case [(:type op) (:f op)]
              [:ok :read]
              (recur history (conj reads {:confirmed confirmed
                                          :possible possible
                                          :actual (:value op)}) possible confirmed)

              [:invoke :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              conj (:v (:value op))) confirmed)

              [:fail :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              (partial remove (partial = (:v (:value op)))))
                     confirmed)

              [:ok :assoc]
              (recur history reads (update-in possible [(:k (:value op))]
                                              (partial remove (partial = (:v (:value op)))))
                     (assoc confirmed (:k (:value op)) (:v (:value op))))

              (recur history reads possible confirmed))))))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
           1
           (/ a b)))

(def total-queue
  "What goes in *must* come out. Verifies that every successful enqueue has a
  successful dequeue. Queues only obey this property if the history includes
  draining them completely. O(n)."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            enqueues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            dequeues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :dequeue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            ; The OK set is every dequeue which we attempted.
            ok         (multiset/intersect dequeues attempts)

            ; Unexpected records are those we *never* tried to enqueue. Maybe
            ; leftovers from some earlier state. Definitely don't want your
            ; queue emitting records from nowhere!
            unexpected (set/difference (core/set dequeues) (core/set attempts))

            ; Duplicate records are those which were dequeued more times than
            ; they could have been enqueued; but were attempted at least once.
            duplicated (-> dequeues
                           (multiset/minus attempts)
                           (multiset/minus unexpected))

            ; lost records are ones which we definitely enqueued but never
            ; came out.
            lost       (multiset/minus enqueues dequeues)

            ; Recovered records are dequeues where we didn't know if the enqueue
            ; suceeded or not, but an attempt took place.
            recovered  (multiset/minus ok enqueues)]

        {:valid?          (and (empty? lost) (empty? unexpected))
         :lost            lost
         :unexpected      unexpected
         :duplicated      duplicated
         :recovered       recovered
         :ok-frac         (util/fraction (count ok)         (count attempts))
         :unexpected-frac (util/fraction (count unexpected) (count attempts))
         :duplicated-frac (util/fraction (count duplicated) (count attempts))
         :lost-frac       (util/fraction (count lost)       (count attempts))
         :recovered-frac  (util/fraction (count recovered)  (count attempts))}))))

(def counter
  "A counter starts at zero; add operations should increment it by that much,
  and reads should return the present value. This checker validates that at
  each read, the value is at greater than the sum of all :ok increments and
  :invoke decrements, and lower than the sum of all attempted increments and
  :ok decrements.

  When testing Cassandra, we know a :fail increment did not occur, so we should
  decrement the counter by the appropriate amount.

  Returns a map:

  {:valid?              Whether the counter remained within bounds
   :reads               [[lower-bound read-value upper-bound] ...]
   :errors              [[lower-bound read-value upper-bound] ...]
  "
  (reify Checker
    (check [this test model history]
      (loop [history            (seq (history/complete history))
             lower              0             ; Current lower bound on counter
             upper              0             ; Upper bound on counter value
             pending-reads      {}            ; Process ID -> [lower read-val]
             reads              []]           ; Completed [lower val upper]s
          (if (nil? history)
            ; We're done here
            (let [errors (remove (partial apply <=) reads)]
              {:valid?             (empty? errors)
               :reads              reads
               :errors             errors})
            ; But wait, there's more
            (let [op      (first history)
                  history (next history)]
              (case [(:type op) (:f op)]
                [:invoke :read]
                (recur history lower upper
                       (assoc pending-reads (:process op) [[lower upper]])
                       reads)

                [:ok :read]
                (let [read-ranges (get pending-reads (:process op))
                      v (:value op)
                      [l' u'] (first read-ranges)
                      read (or (some (fn [[l u]] (when (<= l v u) [l v u])) read-ranges)
                               [l' v u'])]
                  (recur history lower upper
                         (dissoc pending-reads (:process op))
                         (conj reads read)))

                [:invoke :add]
                (let [value (:value op)
                      [l' u'] (if (> value 0) [lower (+ upper value)] [(+ lower value) upper])]
                  (recur history l' u' (reduce-kv #(assoc %1 %2 (conj %3 [l' u']))
                                                  {} pending-reads)
                         reads))

                [:fail :add]
                (let [value (:value op)
                      [l' u'] (if (> value 0) [lower (- upper value)] [(- lower value) upper])]
                  (recur history l' u' (reduce-kv #(assoc %1 %2 (conj %3 [l' u']))
                                                  {} pending-reads) reads))

                [:ok :add]
                (let [value (:value op)
                      [l' u'] (if (> value 0) [(+ lower value) upper] [lower (+ upper value)])]
                  (recur history l' u' (reduce-kv #(assoc %1 %2 (conj %3 [l' u']))
                                                  {} pending-reads) reads))

                (recur history lower upper pending-reads reads))))))))

(defn compose
  "Takes a map of names to checkers, and returns a checker which runs each
  check (possibly in parallel) and returns a map of names to results; plus a
  top-level :valid? key which is true iff every checker considered the history
  valid."
  [checker-map]
  (reify Checker
    (check [this test model history]
      (let [results (->> checker-map
                         (pmap (fn [[k checker]]
                                 [k (check checker test model history)]))
                         (into {}))]
        (assoc results :valid? (every? :valid? (vals results)))))))

(defn latency-graph
  "Spits out graphs of latencies."
  []
  (reify Checker
    (check [_ test model history]
      (perf/point-graph! test history)
      (perf/quantiles-graph! test history)
      {:valid? true})))

(defn rate-graph
  "Spits out graphs of throughput over time."
  []
  (reify Checker
    (check [_ test model history]
      (perf/rate-graph! test history)
      {:valid? true})))

(defn perf
  "Assorted performance statistics"
  []
  (compose {:latency-graph (latency-graph)
            :rate-graph    (rate-graph)}))
