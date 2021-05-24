(ns jepsen.generator
  "Generates operations for a test. Generators are composable, stateful objects
  which emit operations for processes until they are exhausted, at which point
  they return nil. Generators may sleep when generating operations, to delay
  the rate at which the test proceeds

  Generators do *not* have to emit a :process for their operations; test
  workers will take care of that.

  Every object may act as a generator, and constantly yields itself.

  Big ol box of monads, really."
  (:refer-clojure :exclude [concat delay seq filter])
  (:require [jepsen.util :as util]
            [clojure.core :as c]
            [clojure.tools.logging :refer [info]])
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent CyclicBarrier)))

(defprotocol Generator
  (op [gen test process] "Yields an operation to apply."))

(extend-protocol Generator
  Object
  (op [this test process] this)

  ; Fns can generate ops by being called with test and process, or with no args
  clojure.lang.AFunction
  (op [f test process]
    (try
      (f test process)
      (catch clojure.lang.ArityException e
        (f)))))

(def ^:dynamic *threads*
  "The set of threads which will execute a particular generator. Used
  where all threads must synchronize.")

(def void
  "A generator which terminates immediately"
  (reify Generator
    (op [gen test process])))

(defn delay-fn
  "Every operation from the underlying generator takes (f) seconds longer."
  [f gen]
  (reify Generator
    (op [_ test process]
      (Thread/sleep (* 1000 (f)))
      (op gen test process))))

(defn delay
  "Every operation from the underlying generator takes dt seconds to return."
  [dt gen]
  (delay-fn (constantly dt) gen))

(defn stagger
  "Introduces uniform random timing noise with a mean delay of dt seconds for
  every operation. Delays range from 0 to 2 * dt."
  [dt gen]
  (delay-fn (partial rand (* 2 dt)) gen))

(defn sleep
  "Takes dt seconds, and always produces a nil."
  [dt]
  (delay dt void))

(defn once
  "Wraps another generator, invoking it only once."
  [source]
  (let [emitted (AtomicBoolean. false)]
    (reify Generator
      (op [gen test process]
        (when-not (.get emitted)
          (when-not (.getAndSet emitted true)
            (op source test process)))))))

(defn log*
  "Logs a message every time invoked, and yields nil."
  [msg]
  (reify Generator
    (op [gen test process]
      (info msg)
      nil)))

(defn log
  "Logs a message only once, and yields nil."
  [msg]
  (once (log* msg)))

(defn each-
  "Takes a function that yields a generator. Invokes that function to
  create a new generator for each distinct process."
  [gen-fn]
  (let [processes (atom {})]
    (reify Generator
      (op [this test process]
        (if-let [gen (get @processes process)]
          (op gen test process)
          (do
            (swap! processes (fn [processes]
                               (if (contains? processes process)
                                 processes
                                 (assoc processes process (gen-fn)))))
            (recur test process)))))))

(defmacro each
  "Takes an expression evaluating to a generator. Captures that expression as a
  function, and constructs a generator that invokes that expression once for
  each process, as new processes arrive, such that each process sees an
  independent copy of the underlying generator."
  [gen-expr]
  `(each- (fn [] ~gen-expr)))

(defn seq
  "Given a sequence of generators, emits one operation from the first, then one
  from the second, then one from the third, etc. If a generator yields nil,
  immediately moves to the next. Yields nil once coll is exhausted."
  [coll]
  (let [elements (atom (cons nil coll))]
    (reify Generator
      (op [this test process]
        (when-let [gen (first (swap! elements next))]
          (if-let [op (op gen test process)]
            op
            (recur test process)))))))

(defn start-stop
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  [t1 t2]
  (seq (sleep t1)
       {:type :info :f :start}
       (sleep t2)
       {:type :info :f :stop}))

(defn mix
  "A random mixture of operations. Takes a collection of generators and chooses
  between them uniformly."
  [gens]
  (let [gens (vec gens)]
    (reify Generator
      (op [_ test process]
        (op (rand-nth gens) test process)))))

(def cas
  "Random cas/read ops for a compare-and-set register over a small field of
  integers."
  (reify Generator
    (op [generator test process]
      (condp < (rand)
        0.66 {:type  :invoke
              :f     :read}
        0.33 {:type  :invoke
              :f     :write
              :value (rand-int 5)}
        {:type  :invoke
         :f     :cas
         :value [(rand-int 5) (rand-int 5)]}))))

(defn queue
  "A random mix of enqueue/dequeue operations over consecutive integers."
  []
  (let [i (atom -1)]
    (reify Generator
      (op [gen test process]
        (if (< 0.5 (rand))
          {:type  :invoke
           :f     :enqueue
           :value (swap! i inc)}
          {:type  :invoke
           :f     :dequeue})))))

(defn drain-queue
  "Wraps a generator, and keeps track of the balance of :enqueue and :dequeue
  operations that pass through. When the underlying generator is exhausted,
  emits enough :dequeue operations to dequeue every attempted enqueue."
  [gen]
  (let [outstanding (atom 0)]
    (reify Generator
      (op [_ test process]
        (if-let [op (op gen test process)]
          (do (when (= :enqueue (:f op))
                (swap! outstanding inc))
              op)

          ; Exhausted
          (when (pos? (swap! outstanding dec))
            {:type :invoke :f :dequeue}))))))

(defn limit
  "Takes a generator and returns a generator which only produces n operations."
  [n gen]
  (let [life (atom (inc n))]
    (reify Generator
      (op [_ test process]
        (when (pos? (swap! life dec))
          (op gen test process))))))

(defn time-limit
  "Yields operations from the underlying generator until dt seconds have
  elapsed."
  [dt source]
  (let [t (atom nil)]
    (reify Generator
      (op [_ test process]
        (when (nil? @t)
          (compare-and-set! t nil (+ (util/linear-time-nanos)
                                     (util/secs->nanos dt))))
        (when (<= (util/linear-time-nanos) @t)
          (op source test process))))))

(defn filter
  "Takes a generator and yields a generator which emits only operations
  satisfying `(f op)`."
  [f gen]
  (reify Generator
    (op [_ test process]
      (loop []
        (when-let [op' (op gen test process)]
          (if (f op')
            op'
            (recur)))))))

(defn on
  [f source]
  "Forwards operations to source generator iff (f process) is true. Rebinds
  *threads* appropriately."
  (reify Generator
    (op [gen test process]
      (when (f process)
        (binding [*threads* (c/filter f *threads*)]
          (op source test process))))))

(defn concat
  "Takes n generators and yields the first non-nil operation from any, in
  order."
  [& sources]
  (reify Generator
    (op [gen test process]
      (loop [[source & sources] sources]
        (when source
          (if-let [op (op source test process)]
            op
            (recur sources)))))))

(defn conductor
  "Combines a generator of normal operations and a generator for a conductor.
  The name of the conductor is given."
  ([conductor conductor-gen]
   (on #{conductor} conductor-gen))
  ([conductor conductor-gen src-gen]
   (concat (on #{conductor} conductor-gen)
           (on (complement #{conductor}) src-gen))))

(def nemesis
  "Combines a generator of normal operations and a generator for nemesis
  operations into one. When the process requesting an operation is :nemesis,
  routes to the nemesis generator; otherwise to the normal generator."
  (partial conductor :nemesis))

(defn clients
  "Executes generator only on clients."
  [client-gen]
  (on number? client-gen))

(defn synchronize
  "Blocks until all nodes are blocked awaiting operations from this generator,
  then allows them to proceed. Only synchronizes a single time; subsequent
  operations on this generator proceed freely."
  [gen]
  (let [state (atom :fresh)]
    (reify Generator
      (op [_ test process]
        (when (not= :clear @state)
          ; Ensure a barrier exists
          (compare-and-set! state :fresh
                            (CyclicBarrier. (count *threads*)
                                            (partial reset! state :clear)))

          ; Block on barrier
          (.await ^CyclicBarrier @state))
        (op gen test process)))))

(defn phases
  "Like concat, but requires that all threads finish the first generator before
  moving to the second, and so on."
  [& generators]
  (apply concat (map synchronize generators)))

(defn then
  "Generator B, synchronize, then generator A. Why is this backwards? Because
  it reads better in ->> composition."
  [a b]
  (concat b (synchronize a)))

(defn singlethreaded
  "Obtaining an operation from the underlying generator requires an exclusive
  lock."
  [gen]
  (reify Generator
    (op [this test process]
      (locking this
        (op gen test process)))))

(defn barrier
  "When the given generator completes, synchronizes, then yields nil."
  [gen]
  (->> gen (then void)))
