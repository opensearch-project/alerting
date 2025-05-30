// TimeOutableListener.kt
package org.opensearch.alerting.listener

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.action.ActionListener
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

/**
 * The wrapper that schedules a timeout and wraps it in an actual [ActionListener]. The [TimeOutWrappedListener]
 * guarantees that only one of timeout/response/failure gets executed so that responses are not sent over a closed channel
 * The wrap and actual scheduling has been split to avoid races in listeners as they get attached.
 */
class TimeOutWrappedListener {
    companion object {
        private val logger: Logger = LogManager.getLogger(TimeOutWrappedListener::class.java)

        /**
         * Wraps the listener and schedules the timeout
         */
        fun <Response> wrapScheduledTimeout(
            threadPool: ThreadPool,
            timeout: TimeValue,
            executor: String,
            actionListener: ActionListener<Response>,
            timeoutConsumer: Consumer<ActionListener<Response>>,
        ): PrioritizedActionListener<Response> {
            return scheduleTimeout(threadPool, timeout, executor, initListener(actionListener, timeoutConsumer))
        }

        fun <Response> initListener(
            actionListener: ActionListener<Response>,
            timeoutConsumer: Consumer<ActionListener<Response>>,
        ): PrioritizedActionListener<Response> {
            return CompletionPrioritizedActionListener(actionListener, timeoutConsumer)
        }

        fun <Response> scheduleTimeout(
            threadPool: ThreadPool,
            timeout: TimeValue,
            executor: String,
            completionTimeoutListener: PrioritizedActionListener<Response>,
        ): PrioritizedActionListener<Response> {
            (completionTimeoutListener as CompletionPrioritizedActionListener<Response>).cancellable =
                threadPool.schedule(completionTimeoutListener as Runnable, timeout, executor)
            return completionTimeoutListener
        }

        private class CompletionPrioritizedActionListener<Response>(
            private val actionListener: ActionListener<Response>,
            private val timeoutConsumer: Consumer<ActionListener<Response>>,
        ) : PrioritizedActionListener<Response>, Runnable {
            @Volatile
            var cancellable: Scheduler.ScheduledCancellable? = null
            private val complete = AtomicBoolean(false)

            private fun cancel() {
                if (cancellable != null && cancellable!!.isCancelled == false) {
                    cancellable!!.cancel()
                }
            }

            override fun run() {
                executeImmediately()
            }

            override fun executeImmediately() {
                if (complete.compareAndSet(false, true)) {
                    cancel()
                    timeoutConsumer.accept(this)
                }
            }

            override fun onResponse(response: Response) {
                if (complete.compareAndSet(false, true)) {
                    cancel()
                    actionListener.onResponse(response)
                }
            }

            override fun onFailure(e: Exception) {
                if (complete.compareAndSet(false, true)) {
                    cancel()
                    actionListener.onFailure(e)
                }
            }
        }
    }
}
