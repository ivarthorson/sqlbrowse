(ns net.roboloco.databrowser.middleware
  (:require [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.reload :refer [wrap-reload]]
            [ring.middleware.defaults :refer [site-defaults wrap-defaults]]
            [ring.middleware.json :refer [wrap-json-params
                                          wrap-json-body
                                          wrap-json-response]]            
            [prone.middleware :refer [wrap-exceptions]]))

(def middleware
  [#(wrap-defaults % (-> (assoc site-defaults
                           :security {:anti-forgery false
                                      :xss-protection {:enable true :mode :block}
                                      :frame-options :sameorigin
                                      :content-type-options :nosniff})
                         (assoc-in [:session :cookie-attrs :same-site] :lax)))
   wrap-json-body
   wrap-json-response
   wrap-params
   ;; For debugging
   wrap-exceptions
   wrap-reload])
