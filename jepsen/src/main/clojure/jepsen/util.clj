(ns jepsen.util
  "Kitchen sink"
  (:require [clojure.tools.logging :refer [info]]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [clj-time.core :as time]
            [clj-time.local :as time.local]
            [knossos.history :as history])
  (:import (java.util.concurrent.locks LockSupport)))

(defn majority
  "Given a number, returns the smallest integer strictly greater than half."
  [n]
  (inc (int (Math/floor (/ n 2)))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
    1
    (/ a b)))

(defn inc*
  "Like inc, but (inc nil) => 1."
  [x]
  (if (nil? x)
    1
    (inc x)))

(defn local-time
  "Drops millisecond resolution"
  []
  (let [t (time.local/local-now)]
    (time/minus t (time/millis (time/milli t)))))

(defn op->str
  "Format an operation as a string."
  [op]
  (str (:process op)         \tab
       (:type op)            \tab
       (pr-str (:f op))      \tab
       (pr-str (:value op))
       (when-let [err (:error op)]
         (str \tab err))))

(defn print-history
  "Prints a history to the console."
  [history]
  (doseq [op history]
    (println (op->str op))))

(defn log-op
  "Logs an operation and returns it."
  [op]
  (info (op->str op))
  op)

(def logger (agent nil))
(defn log-print
      [_ & things]
      (apply println things))
(defn log
      [& things]
      (apply send-off logger log-print things))

;(defn all-loggers []
;  (->> (org.apache.log4j.LogManager/getCurrentLoggers)
;       (java.util.Collections/list)
;       (cons (org.apache.log4j.LogManager/getRootLogger))))

(defn all-jdk-loggers []
  (let [manager (java.util.logging.LogManager/getLogManager)]
    (->> manager
         .getLoggerNames
         java.util.Collections/list
         (map #(.getLogger manager %)))))

(defmacro mute-jdk [& body]
  `(let [loggers# (all-jdk-loggers)
         levels#  (map #(.getLevel %) loggers#)]
     (try
       (doseq [l# loggers#]
         (.setLevel l# java.util.logging.Level/OFF))
       ~@body
       (finally
         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
                     loggers#
                     levels#))))))

;(defmacro mute-log4j [& body]
;  `(let [loggers# (all-loggers)
;         levels#  (map #(.getLevel %) loggers#)]
;     (try
;       (doseq [l# loggers#]
;         (.setLevel l# org.apache.log4j.Level/OFF))
;       ~@body
;       (finally
;         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
;                     loggers#
;                     levels#))))))

(defmacro mute [& body]
  `(mute-jdk
;     (mute-log4j
       ~@body));)

(defn ms->nanos [ms] (* ms 1000000))

(defn nanos->ms [nanos] (/ nanos 1000000))

(defn secs->nanos [s] (* s 1e9))

(defn nanos->secs [nanos] (/ nanos 1e9))

(defn ^Long linear-time-nanos
  "A linear time source in nanoseconds."
  []
  (System/nanoTime))

(def ^:dynamic ^Long *relative-time-origin*
  "A reference point for measuring time in a test run.")

(defmacro with-relative-time
  "Binds *relative-time-origin* at the start of body."
  [& body]
  `(binding [*relative-time-origin* (linear-time-nanos)]
     ~@body))

(defn relative-time-nanos
  "Time in nanoseconds since *relative-time-origin*"
  []
  (- (linear-time-nanos) *relative-time-origin*))

(defn sleep
  "High-resolution sleep; takes a (possibly fractional) time in ms."
  [dt]
  (let [t (+ (long (ms->nanos dt))
                    (System/nanoTime))]
    (while (< (+ (System/nanoTime) 10000) t)
      (LockSupport/parkNanos (- t (System/nanoTime))))))

(defmacro time-
  [& body]
  `(let [t0# (System/nanoTime)]
    ~@body
     (nanos->ms (- (System/nanoTime) t0#))))

(defmacro unwrap-exception
  "Catches exceptions from body and re-throws their causes. Useful when you
  don't want the wrapper from, say, a future's exception handler."
  [& body]
  `(try ~@body
        (catch Throwable t#
          (throw (.getCause t#)))))

(defmacro timeout
  "Times out body after n millis, returning timeout-val."
  [millis timeout-val & body]
  `(let [thread# (promise)
         worker# (future
                   (deliver thread# (Thread/currentThread))
                   ~@body)
         retval# (unwrap-exception
                   (deref worker# ~millis ::timeout))]
     (if (= retval# ::timeout)
       (do ; Can never remember which does which
           (.interrupt @thread#)
           (future-cancel worker#)
           ~timeout-val)
       retval#)))

(defn map-kv
  "Takes a function (f [k v]) which returns [k v], and builds a new map by
  applying f to every pair."
  [f m]
  (into {} (r/map f m)))

(defn map-vals
  "Maps values in a map."
  [f m]
  (map-kv (fn [[k v]] [k (f v)]) m))

(defn integer-interval-set-str
  "Takes a set of integers and yields a sorted, compact string representation."
  [set]
  (assert (not-any? nil? set))
       (let [[runs start end]
             (reduce (fn r [[runs start end] cur]
                       (cond ; Start new run
                             (nil? start) [runs cur cur]

                             ; Continue run
                             (= cur (inc end)) [runs start cur]

                             ; Break!
                             :else [(conj runs [start end]) cur cur]))
                     [[] nil nil]
                     (sort set))
             runs (if (nil? start) runs (conj runs [start end]))]
         (str "#{"
              (->> runs
                   (map (fn m [[start end]]
                          (if (= start end)
                            start
                            (str start ".." end))))
                   (str/join " "))
              "}")))

(defmacro meh
  "Returns, rather than throws, exceptions."
  [& body]
  `(try ~@body (catch Exception e# e#)))

(defmacro with-thread-name
  "Sets the thread name for duration of block."
  [thread-name & body]
  `(let [old-name# (.. Thread currentThread getName)]
     (try
       (.. Thread currentThread (setName (name ~thread-name)))
       ~@body
       (finally (.. Thread currentThread (setName old-name#))))))

(defn maybe-number
  "Tries reading a string as a long, then double, then string. Passes through
  nil. Useful for getting nice values out of stats APIs that just dump a bunch
  of heterogenously-typed strings at you."
  [s]
  (when s
    (try (Long/parseLong s)
         (catch java.lang.NumberFormatException e
           (try (Double/parseDouble s)
                (catch java.lang.NumberFormatException e
                  s))))))

(defn coll
  "Wraps non-coll things into singleton lists, and leaves colls as themselves.
  Useful when you can take either a single thing or a sequence of things."
  [thing-or-things]
  (cond (nil? thing-or-things)  nil
        (coll? thing-or-things) thing-or-things
        true                    (list thing-or-things)))

(defn sequential
  "Wraps non-sequential things into singleton lists, and leaves sequential
  things or nil as themselves. Useful when you can take either a single thing
  or a sequence of things."
  [thing-or-things]
  (cond (nil? thing-or-things)        nil
        (sequential? thing-or-things) thing-or-things
        true                          (list thing-or-things)))

(defn history->latencies
  "Takes a history--a sequence of operations--and emits the same history but
  with every invocation containing two new keys:

  :latency    the time in nanoseconds it took for the operation to complete.
  :completion the next event for that process"
  [history]
  (let [idx (->> history
                 (map-indexed (fn [i op] [op i]))
                 (into {}))]
    (->> history
         (reduce (fn [[history invokes] op]
                   (if (= :invoke (:type op))
                     ; New invocation!
                     [(conj! history op)
                      (assoc! invokes (:process op)
                              (dec (count history)))]

                     (if-let [invoke-idx (get invokes (:process op))]
                       ; We have an invocation for this process
                       (let [invoke (get history invoke-idx)
                             ; Compute latency
                             l    (- (:time op) (:time invoke))
                             op (assoc op :latency l)]
                         [(-> history
                              (assoc! invoke-idx
                                      (assoc invoke :latency l, :completion op))
                              (conj! op))
                          (dissoc! invokes (:process op))])

                       ; We have no invocation for this process
                       [(conj! history op) invokes])))
                 [(transient []) (transient {})])
         first
         persistent!)))

(defn nemesis-intervals
  "Given a history where a nemesis goes through :f :start and :f :stop
  transitions, constructs a sequence of pairs of :start and :stop ops. Since a
  nemesis usually goes :start :start :stop :stop, we construct pairs of the
  first and third, then second and fourth events. Where no :stop op is present,
  we emit a pair like [start nil]."
  [history]
  (let [[pairs starts] (->> history
                            (filter #(= :nemesis (:process %)))
                            (reduce (fn [[pairs starts] op]
                                      (case (:f op)
                                        :start [pairs (conj starts op)]
                                        :stop  [(conj pairs [(peek starts)
                                                             op])
                                                (pop starts)]
                                        [pairs starts]))
                                    [[] (clojure.lang.PersistentQueue/EMPTY)]))]
    (concat pairs (map vector starts (repeat nil)))))

(defn longest-common-prefix
  "Given a collection of sequences, finds the longest sequence which is a
  prefix of every sequence given."
  [cs]
  (when (seq cs)
    (reduce (fn prefix [s1 s2]
              (let [len (->> (map = s1 s2)
                             (take-while true?)
                             count)]
                ; Avoid unnecessary seq wrapping
                (if (= len (count s1))
                  s1
                  (take len s2))))
            cs)))

(defn drop-common-proper-prefix
  "Given a collection of sequences, removes the longest common proper prefix
  from each one."
  [cs]
  (map (partial drop (reduce min
                             (count (longest-common-prefix cs))
                             (map (comp dec count) cs)))
       cs))

(defn- identical?-seq-new
  "Finds new items in seq b that aren't identical? to any in a"
  [a b]
  (filter #(not-any? (partial identical? %) a) b))

(defn- checked-watch-add
  "Checked add-watch with backfilling"
  [watched key watcher-fn]
  (let [preparing (atom true)
        before @watched
        delta (atom nil)]
    (add-watch watched key
               (fn [k r o n]
                 (while @preparing
                   (Thread/sleep 1000))
                 (doseq [new (identical?-seq-new o n)]
                   (when-not (some (partial identical? new) @delta)
                     (watcher-fn new)))))
    (let [after @watched]
      (reset! delta (set (identical?-seq-new before after)))
      (doseq [item after]
        (watcher-fn item))
      (reset! preparing false))))

(defn add-history-callback
  [test key ^java.util.function.Consumer callback]
  (checked-watch-add (get test :active-histories)
                     (keyword (str (name :add-callbacks) \- (name key)))
                     #(checked-watch-add % key (fn [op] (.accept callback op)))))
