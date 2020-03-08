(ns net.roboloco.databrowser.handlers
  (:require [clojure.java.io :as io]
            [clojure.core.matrix :as matrix]
            [clojure.string :as string]            
            [config.core :refer [env]]
            [hiccup.page :refer [include-js include-css html5]]
            [reitit.ring :as reitit-ring]
            [ring.middleware.file :as ring-file]
            [ring.util.response :as response]
            [ring.util.mime-type :as mime-type]
            [net.roboloco.databrowser.csv :as csv]
            [net.roboloco.databrowser.middleware :refer [middleware]]
            [net.roboloco.databrowser.schema :as schema]
            [net.roboloco.databrowser.sql :as sql]
            [net.roboloco.databrowser.util :as util]
            [net.roboloco.databrowser.zip :as zip]))

(set! *warn-on-reflection* true)

(def file-dir (or (System/getenv "FILE_DIR" "/path/to/some/dir")))

(defn file-path-for-id 
  "Returns the file path corresponding to a given ID."
  [id]
  (str file-dir "/" id))

(def ^:dynamic *db* sql/default-db)

(defn index-page []
  (html5
   [:head
    [:meta {:charset "utf-8"}
     :meta {:name "viewport"
            :content "width=device-width, initial-scale=1"}]
    (include-css (if (env :dev)
                   "/css/site.css"
                   "/css/site.min.css"))
    (include-css "/css/slider.css")]   
   [:body {:class "body-container"}
    [:h3 "Loading..."]
    [:div#app]]))

;; -----------------------------------------------------------------------------

;; TODO: Remove this and instead verify that it's not bullshit
(defn range-min-max-to-doubles
  "Converts the :range-min and :range-max things into numbers."
  [h]
  (into {} (map (fn [[k v]]
                  (if (and (string? v)
                           (#{:range-min :range-max} k))
                    [k (Double/parseDouble v)]
                    [k v])))))


;; TODO: move to handler side
(defn get-histogram-json
  "Returns a JSON hashmap ready to return a "
  [db table column & [n-bins selected-ids]]
  (->> (value-histogram db table column nil selected-ids)
       (sql-results-to-json)))

;; -----------------------------------------------------------------------------
;; HTTP Request Argument-Parsing Functions

;; TODO: REFACTOR! A better way would be to run these against a spec, rather
;; than defining a custom validator for every single parameter. That would work in 
;; all cases except for the "table-and-column" case.

;; Furthermore, you could also do the handler logic that returns a 400 error
;; if the error is not valid.

(defn- extract-body-parameter
  "Extracts the body parameter under a given parameter-name, which may be
  either a string or a keyword."
  [request paremeter-name]
  (when-let [body (:body request)]
    (when-let [parameter (get body (name parameter-name))]
      parameter)))

(defn extract-validated-filters
  "Extracts the validated filters parameters from the response."
  [request]
  (when-let [filters (extract-body-parameter "filters")]    
    ;; TODO: sanitize filters forcefully
    (mapv keys-to-keywords filters)))

(defn extract-validated-table-and-column
  "Extracts the validated table and column parameters from the response."
  [request]
  (let [table (extract-body-parameter "table")
        column (extract-body-parameter "column")]
    (when (and table
               column
               (sql/valid-table? table)
               (sql/valid-column? table column))
      [table column])))

(defn extract-validated-id
  "Extracts the validated ids parameter from the response."
  [request]
  (when-let [id (extract-body-parameter "id")]    
    ;; TODO: sanitize ids forcefully
    id))

(defn extract-validated-ids
  "Extracts the validated ids parameter from the response."
  [request]
  (when-let [ids (extract-body-parameter "ids")]    
    ;; TODO: sanitize ids forcefully
    ids))

(defn extract-validated-fulltext
  "Extracts the validated ids parameter from the response."
  [request]
  (when-let [fulltext (extract-body-parameter "fulltext")]
    ;; TODO: Sanitize forcefully
    fulltext))

;; -----------------------------------------------------------------------------
;; Route handlers

(defn index-page-handler
  "Serves up the index page."
  [request]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (index-page)})

(defn schema-handler
  "Returns a JSON representation of the database schema."
  [request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (sql/get-schema *db*)})

(defn frequencies-handler
  "Returns a JSON containing the frequencies of values in a categorical column."
  [request]
  (let [[table column] (extract-validated-table-and-column request)
        ids (extract-validated-ids request)]
    (if (and table column (= :categorical 
                             (schema/lookup-column-type *db* table column)))
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (sql/value-frequencies-in-column *db* table column)}
      {:status 400
       :headers {"Content-Type" "text/plain"}
       :body "Table, column, or ids parameters are invalid."})))

(defn histogram-handler
  "Returns a JSON containing a histogram of the values in a continuous column."
  [request]
  (let [[table column] (extract-validated-table-and-column request)
        ids (extract-validated-ids request)]
     (if (and table column (= :continuous
                             (schema/lookup-column-type *db* table column)))
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (sql/value-histogram-of-column *db* table column)}
      {:status 400
       :headers {"Content-Type" "text/plain"}
       :body "Table, column, or ids parameters are invalid."})))

(defn search-handler
  "Search for ID numbers that match all of the filters."
  [request]
  (if-let [filters (etxract-validated-filters request)]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (sql/search-with-filters *db* filters)}
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Filters parameter is invalid."}))

(defn info-handler
  "Returns all information we have about a specific id."
  [request]
  (if-let [id (extract-validated-id request)]
   {:status 200
    :headers {"Content-Type" "application/json"}
    :body (sql/query-all-tables-for-id *db* id)}
   {:status 400
    :headers {"Content-Type" "text/plain"}
    :body "ID parameter is invalid."}))

(defn file-handler
  "Streams a file out for download when given a specific id."
  [request]
  (let [id (extract-validated-id request)
        filepath (file-path-for-id id)
        file (io/file filepath)]
    (if (and id (.exists file))
      {:status 200
       :headers {"Content-Type" "text/plain"
                 "Content-Disposition" (format "attachment; filename=\"%s\"" id)}
       :body (io/input-stream file)}
      {:status 400
       :headers {"Content-Type" "text/plain"}
       :body "Invalid ID parameter, or matching file not found."})))

(defn csv-building-handler
  "Builds a CSV from all the selected IDs. NOT THREADSAFE."
  [request]
  (if-let [ids (extract-validated-ids request)]
    (let [row-hashes (for [id ids]
                       (assoc (csv/flatten-keys
                               (csv/merge-list-values
                                (sql/query-all-tables-for-id *db* id)))
                         "id" id))
          csv-filename "mycsv.csv" ;; TODO
          csv-filepath "/tmp/mycsv.csv" ;; TODO: Use tmp directory and uuid
          _ (csv/save-csv (csv/maps->tabular row-hashes) csv-filepath)] ;; TODO: Not this
      {:status 200
       :headers {"Content-Type" "text/plain"
                 "Content-Disposition" (format "attachment; filename=\"%s\"" csv-filename)}
       :body (io/input-stream (io/file csv-filepath))})
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Invalid ids parameter, or matching file not found."}))

;; -----------------------------------------------------------------------------
;; This is a patch that needs to be submitted to reitit-ring

(defn create-file-handler
  [{:keys [parameter root path allow-symlinks? index-files? paths not-found-handler]
    :or [parameter (keyword "")
         root "public"
         index-files? false
         paths (constantly nil)
         not-found-handler (constantly {:status 404, :body "", :headers {}})]}]
  (let [options {:root root, :index-files? index-files?, :allow-symlinks? allow-symlinks?}
        path-size (count path)
        create (fn [handler]
                 (fn 
                   ([request] (handler request))
                   ([request respond _] (respond (handler request)))))
        join-paths (fn [& paths]
                     (string/replace (string/replace (string/join "/" paths)
                                                     #"([/]+)" "/")
                                     #"/$" ""))
        file-response (fn [path]
                        (when-let [response (or (paths (join-paths "/" path))
                                                (response/file-response path options))]
                          (response/content-type response (mime-type/ext-mime-type path))))
        handler (if path
                  (fn [request]
                    (let [uri (:uri request)]
                      (when-let [path (when (>= (count uri) path-size)
                                        (subs uri path-size))]
                        (file-response path))))
                  (fn [request]
                    (let [uri (:uri request)
                          path (-> request :path-params parameter)]
                      (or (file-response path uri)
                          (not-found-handler request)))))]
    (create handler)))

(defn wrap-content-disposition-so-it-downloads
  [handler] 
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Content-Disposition"] "attachment"))))

;; -----------------------------------------------------------------------------

(def app
  (reitit-ring/ring-handler
   (reitit-ring/router
    [["/"            {:get {:handler index-page-handler}}
      "/schema.json" {:get {:handler schema-handler}}
      "/info"        {:post {:handler info-handler}}
      "/csv"         {:post {:handler csv-building-handler}}
      "/frequencies" {:post {:handler frequencies-handler}}
      "/histogram"   {:post {:handler histogram-handler}}
      "/composite"   {:post {:handler composite-handler}}
      "/search"      {:post {:handler search-handler}}]])
   (reitit-ring/routes
    (wrap-content-disposition-so-it-downloads
     (create-file-handler {:path "/files" :root file-dir})
     (reitit-ring/create-resource-handler {:path "/" :root "/public"})
     (reitit-ring/create-default-handler))
    {:middleware middleware})))

