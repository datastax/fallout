(ns clojure.core.rrb-vector.transients
  (:require [clojure.core.rrb-vector.nodes :refer [ranges last-range]])
  (:import (clojure.core.rrb_vector.nodes NodeManager)
           (clojure.core ArrayManager)
           (java.util.concurrent.atomic AtomicReference)))

(definterface ITransientHelper
  (editableRoot [^clojure.core.rrb_vector.nodes.NodeManager nm
                 ^clojure.core.ArrayManager am
                 root])
  (editableTail [^clojure.core.ArrayManager am
                 tail])
  (ensureEditable [^clojure.core.rrb_vector.nodes.NodeManager nm
                   root])
  (ensureEditable [^clojure.core.rrb_vector.nodes.NodeManager nm
                   ^clojure.core.ArrayManager am
                   ^java.util.concurrent.atomic.AtomicReference root-edit
                   current-node
                   ^int shift])
  (pushTail [^clojure.core.rrb_vector.nodes.NodeManager nm
             ^clojure.core.ArrayManager am
             ^int shift
             ^int cnt
             ^java.util.concurrent.atomic.AtomicReference root-edit
             current-node
             tail-node])
  (popTail [^clojure.core.rrb_vector.nodes.NodeManager nm
            ^clojure.core.ArrayManager am
            ^int shift
            ^int cnt
            ^java.util.concurrent.atomic.AtomicReference root-edit
            current-node])
  (doAssoc [^clojure.core.rrb_vector.nodes.NodeManager nm
            ^clojure.core.ArrayManager am
            ^int shift
            ^java.util.concurrent.atomic.AtomicReference root-edit
            current-node
            ^int i
            val])
  (newPath [^clojure.core.rrb_vector.nodes.NodeManager nm
            ^clojure.core.ArrayManager am
            tail
            ^java.util.concurrent.atomic.AtomicReference edit
            ^int shift
            current-node]))

(def ^ITransientHelper transient-helper
  (reify ITransientHelper
    (editableRoot [this nm am root]
      (.node nm
             (AtomicReference. (Thread/currentThread))
             (clojure.core/aclone ^objects (.array nm root))))

    (editableTail [this am tail]
      (let [ret (.array am 32)]
        (System/arraycopy tail 0 ret 0 (.alength am tail))
        ret))

    (ensureEditable [this nm root]
      (let [owner (->> root (.edit nm) (.get))]
        (cond
          (identical? owner (Thread/currentThread))
          nil

          (not (nil? owner))
          (throw
           (IllegalAccessError. "Transient used by non-owner thread"))

          :else
          (throw
           (IllegalAccessError. "Transient used after persistent! call")))))

    (ensureEditable [this nm am root-edit current-node shift]
      (if (identical? root-edit (.edit nm current-node))
        current-node
        (if (zero? shift)
          (let [new-arr (.aclone am (.array nm current-node))]
            (.node nm root-edit new-arr))
          (let [new-arr (aclone ^objects (.array nm current-node))]
            (if (== 33 (alength ^objects new-arr))
              (aset new-arr 32 (aclone (ints (aget ^objects new-arr 32)))))
            (.node nm root-edit new-arr)))))

    (pushTail [this nm am shift cnt root-edit current-node tail-node]
      (let [ret (.ensureEditable this nm am root-edit current-node shift)]
        (if (.regular nm ret)
          (do (loop [n ret shift shift]
                (let [arr    (.array nm n)
                      subidx (bit-and (bit-shift-right (dec cnt) shift) 0x1f)]
                  (if (== shift 5)
                    (aset ^objects arr subidx tail-node)
                    (let [child (aget ^objects arr subidx)]
                      (if (nil? child)
                        (aset ^objects arr subidx
                              (.newPath this nm am
                                        (.array nm tail-node)
                                        root-edit
                                        (unchecked-subtract-int shift 5)
                                        tail-node))
                        (let [editable-child
                              (.ensureEditable this nm am
                                               root-edit
                                               child
                                               (unchecked-subtract-int
                                                shift 5))]
                          (aset ^objects arr subidx editable-child)
                          (recur editable-child (- shift 5))))))))
              ret)
          (let [arr  (.array nm ret)
                rngs (ranges nm ret)
                li   (unchecked-dec-int (aget rngs 32))
                cret (if (== shift 5)
                       nil
                       (let [child (.ensureEditable this nm am
                                                    root-edit
                                                    (aget ^objects arr li)
                                                    (unchecked-subtract-int
                                                     shift 5))
                             ccnt  (if (pos? li)
                                     (unchecked-subtract-int
                                      (aget rngs li)
                                      (aget rngs (unchecked-dec-int li)))
                                     (aget rngs 0))]
                         (if-not (== ccnt (bit-shift-left 1 shift))
                           (.pushTail this nm am
                                      (unchecked-subtract-int shift 5)
                                      (unchecked-inc-int ccnt)
                                      root-edit
                                      child
                                      tail-node))))]
            (if cret
              (do (aset ^objects arr li cret)
                  (aset rngs li (unchecked-add-int (aget rngs li) 32))
                  ret)
              (do (aset ^objects arr (inc li)
                        (.newPath this nm am
                                  (.array nm tail-node)
                                  root-edit
                                  (unchecked-subtract-int shift 5)
                                  tail-node))
                  (aset rngs (unchecked-inc-int li)
                        (unchecked-add-int (aget rngs li) 32))
                  (aset rngs 32 (unchecked-inc-int (aget rngs 32)))
                  ret))))))

    (popTail [this nm am shift cnt root-edit current-node]
      (let [ret (.ensureEditable this nm am root-edit current-node shift)]
        (if (.regular nm ret)
          (let [subidx (bit-and
                        (bit-shift-right (unchecked-dec-int cnt) shift)
                        0x1f)]
            (cond
              (> shift 5)
              (let [child (.popTail this nm am
                                    (unchecked-subtract-int shift 5)
                                    cnt
                                    root-edit
                                    (aget ^objects (.array nm ret) subidx))]
                (if (and (nil? child) (zero? subidx))
                  nil
                  (let [arr (.array nm ret)]
                    (aset ^objects arr subidx child)
                    ret)))

              (zero? subidx)
              nil

              :else
              (let [arr (.array nm ret)]
                (aset ^objects arr subidx nil)
                ret)))
          (let [rngs   (ranges nm ret)
                subidx (bit-and
                        (bit-shift-right (unchecked-dec-int cnt) shift)
                        0x1f)
                subidx (loop [subidx subidx]
                         (if (or (zero? (aget rngs (unchecked-inc-int subidx)))
                                 (== subidx 31))
                           subidx
                           (recur (unchecked-inc-int subidx))))]
            (cond
              (> shift 5)
              (let [child     (aget ^objects (.array nm ret) subidx)
                    child-cnt (if (zero? subidx)
                                (aget rngs 0)
                                (unchecked-subtract-int
                                 (aget rngs subidx)
                                 (aget rngs (unchecked-dec-int subidx))))
                    new-child (.popTail this nm am
                                        (unchecked-subtract-int subidx 5)
                                        child-cnt
                                        root-edit
                                        child)]
                (cond
                  (and (nil? new-child) (zero? subidx))
                  nil

                  (.regular nm child)
                  (let [arr (.array nm ret)]
                    (aset rngs subidx
                          (unchecked-subtract-int (aget rngs subidx) 32))
                    (aset ^objects arr subidx new-child)
                    (if (nil? new-child)
                      (aset rngs 32 (unchecked-dec-int (aget rngs 32))))
                    ret)

                  :else
                  (let [rng  (last-range nm child)
                        diff (unchecked-subtract-int
                              rng
                              (if new-child (last-range nm new-child) 0))
                        arr  (.array nm ret)]
                    (aset rngs subidx
                          (unchecked-subtract-int (aget rngs subidx) diff))
                    (aset ^objects arr subidx new-child)
                    (if (nil? new-child)
                      (aset rngs 32 (unchecked-dec-int (aget rngs 32))))
                    ret)))

              (zero? subidx)
              nil

              :else
              (let [arr   (.array nm ret)
                    child (aget ^objects arr subidx)]
                (aset ^objects arr subidx nil)
                (aset rngs subidx 0)
                (aset rngs 32     (unchecked-dec-int (aget rngs 32)))
                ret))))))
    
    (doAssoc [this nm am shift root-edit current-node i val]
      (let [ret (.ensureEditable this nm am root-edit current-node shift)]
        (if (.regular nm ret)
          (loop [shift shift
                 node  ret]
            (if (zero? shift)
              (let [arr (.array nm node)]
                (.aset am arr (bit-and i 0x1f) val))
              (let [arr    (.array nm node)
                    subidx (bit-and (bit-shift-right i shift) 0x1f)
                    child  (.ensureEditable this nm am
                                            root-edit
                                            (aget ^objects arr subidx)
                                            shift)]
                (aset ^objects arr subidx child)
                (recur (unchecked-subtract-int shift 5) child))))
          (let [arr    (.array nm ret)
                rngs   (ranges nm ret)
                subidx (bit-and (bit-shift-right i shift) 0x1f)
                subidx (loop [subidx subidx]
                         (if (< i (aget rngs subidx))
                           subidx
                           (recur (unchecked-inc-int subidx))))
                i      (if (zero? subidx)
                         i
                         (unchecked-subtract-int
                          i (aget rngs (unchecked-dec-int subidx))))]
            (aset ^objects arr subidx
                  (.doAssoc this nm am
                            (unchecked-subtract-int shift 5)
                            root-edit
                            (aget ^objects arr subidx)
                            i
                            val))))
        ret))

    (newPath [this nm am tail edit shift current-node]
      (if (== (.alength am tail) 32)
        (loop [s 0 n current-node]
          (if (== s shift)
            n
            (let [arr (object-array 32)
                  ret (.node nm edit arr)]
              (aset ^objects arr 0 n)
              (recur (unchecked-add s 5) ret))))
        (loop [s 0 n current-node]
          (if (== s shift)
            n
            (let [arr  (object-array 33)
                  rngs (int-array 33)
                  ret  (.node nm edit arr)]
              (aset ^objects arr 0 n)
              (aset ^objects arr 32 rngs)
              (aset rngs 32 1)
              (aset rngs 0 (.alength am tail))
              (recur (unchecked-add s 5) ret))))))))
