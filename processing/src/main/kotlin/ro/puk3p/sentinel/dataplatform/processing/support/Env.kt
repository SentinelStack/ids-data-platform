package ro.puk3p.sentinel.dataplatform.processing.support

object Env {
    fun get(key: String, default: String): String =
        System.getenv(key)?.takeIf { it.isNotBlank() } ?: default
}
