(ns net.roboloco.databrowser.schema-autodetection
  (:require [net.roboloco.databrowser.util :as util]))
 
;; TODO:all of these functions accept hashmaps from list-sql-tables,
;; so should be a schema/spec

(defn probably-categorical?
  "Returns TRUE when this column is probably categorical.
  Argument CATEGORICAL-CUTOFF is an integer that "
  [categorical-cutoff h]
  (and (= "text" (:data_type h))
       (< (:num-distinct-values h) categorical-cutoff)))

(defn probably-boolean?
  "Returns TRUE if this column only contains trues and falses."
  [h]
  (and (or (= "text" :data_type h)
           (= "integer" (:data_type h)))
       (= 2 (:num-distinct-values h))))

(defn probably-integers?
  "Returns TRUE if this column only contains integers"
  [h]
  (= "integer" (:data_type h)))

(defn probably-doubles?
  "Returns TRUE if this column only contains doubles or floats."
  [h]
  (= "double precision" :data_type h))

(defn probably-textual?
  "Returns TRUE if this column is text."
  [h]
  (= "text" :data_type h))

;; ------------------------------------------------------------------------------

(defn guess-column-type
  "Returns a keyword indicating the guessed-type of the column hashmap H, 
  which should be the output of (net.roboloco.databrowser.sql/describe-columns)"
  [h & [categorical-cutoff]]
  (cond
   (probably-boolean? h)     :boolean
   (probably-integers? h)    :continuous
   (probably-doubles? h)     :continuous
   (probably-categorical? categorical-cutoff h) :categorical
   (probably-textual? h)     :text
   :otherwise                nil))

;; TODO: Make the rest of the code here into a thread-safe object

(def schema-cache (agent nil)) ;; TODO: Replace with an agent for thread-safety

(defn get-schema
  "Returns a complete schema of the database."
  [db]
  (if-let [schema @schema-cache]
    schema
    (let [tables (vec (map :tablename (list-sql-tables db)))
          schema (into {} (for [table tables]
                            [table (describe-columns db table)]))]
      (reset! schema-cache schema))))


;; TODO: these next three could be much more highly optimized by 
;; caching; see reset! 

(defn valid-table?
  "TRUE when the table-name is a valid table in the schema."
  [table-name]
  (let [schema (get-schema)]
    (set (keys schema))))

(defn valid-column?
  "TRUE when the table-name is a valid table in the schema."
  [table-name column-name]
  (let [schema (get-schema)]
    (set (keys schema))))

(defn lookup-column-type
  "Returns the type of a given table and column."
  [db table column]
  (let [schema (get-schema db)
        columns (get schema (name table))
        column-type-of (into {} (map (fn [h] [(:column_name h) (:column-type h)])
                                     columns))]
    (get column-type-of (name column))))

