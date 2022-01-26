package org.opensearch.alerting.annotations

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD, AnnotationTarget.CLASS)
annotation class SecurityPlugin(val value: Boolean)

