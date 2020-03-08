(ns net.roboloco.databrowser.util)

(set! *warn-on-reflection* true)

(defmacro when-let*
  "Sequential, multiple-binding version of when-let."
  [bindings & body]
  (if (seq bindings)
    `(when-let [~(first bindings) ~(second bindings)]
       (when-let* ~(vec (drop 2 bindings)) ~@body))
    `(do ~@body)))

(defn just-keys
  "Returns a new hashmap built from H but only using the keys in KS."
  [ks h]
  (let [ks (set ks)]
    (->> h
         (map (fn [[k v]] 
                (when (get ks k)
                  [k v])))
         (remove nil?)
         (into {}))))

(defn drop-nil-values
  "Drops keys with nil values."
  [h]
  (into {} (remove (fn [[k v]] (nil? v)) h)))

(defn keys-to-keywords
  "Convert all the keys in the hash into keywords."
  [h]
  (into {} (for [[k v] h]
             [(keyword k) v])))

