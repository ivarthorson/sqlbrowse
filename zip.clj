(ns net.roboloco.databrowser.zip
  (:import [java.util.zip ZipEntry ZipOutputStream]))

(defmacro ^:private with-entry
  [zip entry-name &body]
  `(let [^ZipOutputStream zip# ~zip]
     (.putNextEntry zip# (ZipEntry. ~entry-name))
     ~@body
     (flush)
     (.closeEntry zip#)))

(defn generate-zip-file
  "Generates a zip file from all the input-files. "
  [input-files zip-filepath]
  (with-open [zipstream (ZipOutputStream. (io/output-stream zip-filepath))]
    (doseq [file input-files]
      (with-entry zipstream file
        (io/copy (io/input-stream file) zipstream)))))
