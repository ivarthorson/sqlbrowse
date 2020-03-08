(ns net.roboloco.databrowser.sql
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.jdbc :as sql]
            [net.roboloco.databrowser.schema-autodetection :as autodetect]
            [net.roboloco.databrowser.util :as util]))

(def max-categories 120)

(def default-db {:dbtype "postgresql"
                 :dbname   (or (System/getenv "POSTGRES_DB")   "csv2sql")
                 :host     (or (System/getenv "POSTGRES_HOST") "127.0.0.1")
                 :user     (or (System/getenv "POSTGRES_USER") "postgres")
                 :password (or (System/getenv "POSTGRES_PASS") "mysecretpassword")})

(defn list-sql-tables
  "Returns a seq of all user-created SQL tables in the PostgreSQL database."
  [db]
  (let [cmd "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"]
    (sql/query db cmd)))

(defn distinct-values-in-column
  "Returns a set of the distinct values found in a given column of a table."
  [db table column]
  (let [cmd (format "SELECT DISTINCT %s FROM %s" column table)]
    (set (mapv (keyword column) (sql/query db cmd)))))

(defn value-frequencies-in-column
  "Returns a hashmap mapping values to the number of occurrences of that value,
  in a given column of a table."
  [db table column]
  (let [cmd (format "SELECT %s, COUNT(*) FROM %s GROUP BY %s"
                    column table column)]
    (->> (sql/query db cmd)
         (map (fn [h] [(get h (keyword column)) (get h :count)]))
         (into {}))))

(defn get-quantile
  "Returns a given quantile of a table and column."
  [db table column quantile & [where-clause]]
  (when-not (<= 0 quantile 1)
    (throw (Exception. (str "Quantile must be between 0 and 1:" quantile))))
    (sql/query 
     db (format "SELECT percentile_cont(%f) WITHIN GROUP (ORDER BY %s) FROM %s%s"
                quantile column table
                (if where-clause
                  (format " WHERE %s" where-clause) 
                  ""))))

(defn get-percentiles
  "Returns a list of the percentiles of a column, from 0th percentile to the
  100th percentile, for a total of 101 elements."
  [db table column & [where-clause]]
  (mapv #(get-quantile db table column (* 0.01 %) where-clause)
        (range 101)))

(defn describe-columns
  "Return a hashmap describing the columns of a table in detail."
  [db table]
  (let [cmd (format "SELECT column_name, data_type, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'" table)]
    (->> (sql/query db cmd)
         (mapv (fn [h]
                 (-> h
                     (assoc :num-distinct-values 
                       (distinct-values-in-column db table (:column_name h)))
                     (#(assoc % :column-type (autodetect/guess-column-type %)))))))))

;; -----------------------------------------------------------------------------
;; Schema stuff...should this be an object?


;; TODO: move to another namespace
(defn sql-results-to-json
  "Converts SQL query results into a tabular JSON format."
  [results]
  (let [ks (vec (keys (first results)))
        vs (mapv #(mapv % ks) results)]
    {:columns ks
     :data vs}))

(defn escape-string-for-sql
  [e]
  (str "'" e "'")) ;; TODO: DO BETTER ESCAPING!!!

(defn seq-to-str
  "Convert a sequence of strings into a string with commas separating the seq elements."
  [seq]
  (->> seq
       (map escape-string-for-sql)
       (interpose ", ")
       (apply str)))

(defn build-single-filter-subquery
  "Returns a SQL query using a given hashmap FILTER. A filter must have
  at least these two columns defined:
        :selected-table           Which table to restrict query to.
        :selected-column          Which column to restrict query to. 

  For categorical columns, the following should also be defined::
        :selected-categories      Which values in that column to restrict query to.

  For continuous columns, there should be a min or a max range defined:
        :range-min                The value above which to search
        :range-max                The value below which to search

  For textual columns, this must be defined:
        :fulltext                 The 'LIKE' query to use.

  If not everything required is defined, this function will return a nil."
  [filter]
  (let [{:keys [selected-categories
                selected-column
                selected-table
                range-min
                range-max
                fulltext]} filter]
    (when (and selected-column selected-table)
      (cond       
       fulltext             [(format "SELECT id FROM %s WHERE %s LIKE ?;"
                                     selected-table selected-column) fulltext]

       selected-categories  [(format "SELECT id FROM %s WHERE %s IN (%s)"
                                     selected-table selected-column 
                                     (seq-to-str selected-categories))] ;;TODO: no seq-to-str

       (and range-min 
            range-max)      [(format "SELECT id FROM %s WHERE %s >= ? AND %s <= ?;"
                                        selected-table selected-column selected-column) 
                             range-min range-max]

       range-min            [(format "SELECT id FROM %s WHERE %s >= ?;"
                                     selected-table selected-column) range-min]

       range-max            [(format "SELECT id FROM %s WHERE %s <= ?;"
                                     selected-table selected-column) range-max]
       
       :otherwise           nil))))


;; TODO: Instead of calling this repeatedly and doing a set intersection...
;; ...make a single, mammoth SQL query. See search-filtered.
(defn search-with-filter
  "Returns a list of all IDs where the filter matches."
  [db filter]
  (->> (build-single-filter-subquery filter)
       (sql/query db)
       (mapv :id)))

(defn search-with-filters
  "Takes the set intersection of all the filters."
  [db filters]
  (->> filters
       (map (partial search-single-filter db))
       (map set)
       (reduce clojure.set/intersection)
       (sort)
       (vec)))

(defn- value-histogram-of-column
  "Returns a histogram of a column's values, in n-bins, which defaults to 100."
  [db table column & [n-bins selected-ids]]
  (let [results (sql/query db (format "SELECT MIN(%s), MAX(%s) FROM %s;"
                                      column column table)) ;; TODO: Can this become part of second query?
        min (:min (first results))
        max (:max (first results))
        n-bins (or n-bins 100)
        binsize (/ (- max min) 100)
        qry (format "SELECT bin_floor, bin_ceil, COUNT(*) AS count
                     FROM ( SELECT FLOOR(%s/%f)*%f      AS bin_floor,
                                   FLOOR(%s/%f)*%f + %f AS bin_ceil
                            FROM %s%s) unused_tmp GROUP BY 1, 2 ORDER BY 1;"
                    column binsize binsize
                    column binsize binsize binsize
                    table (if selected-ids
                            (format " WHERE id IN (%s)" (seq-to-str selected-ids)) 
                            ""))]
    (sql/query db qry)))


(defn query-all-tables-for-id
  [db id]
  (let [schema (autodetect/get-schema db)]
    (->> (for [table (map name (keys schema))]
           [table (->> (sql/query db [(format "SELECT * FROM %s WHERE id = ?;" table) id])
                       (mapv util/drop-nil-values))])
         (into {}))))
