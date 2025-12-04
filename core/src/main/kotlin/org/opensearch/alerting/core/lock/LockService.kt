package org.opensearch.alerting.core.lock

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.action.update.UpdateResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.engine.DocumentMissingException
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.transport.client.Client
import java.io.IOException
import java.time.Instant
import java.util.concurrent.TimeUnit

private val log = LogManager.getLogger(LockService::class.java)

class LockService(
    private val client: Client,
    private val clusterService: ClusterService,
) {
    private var testInstant: Instant? = null

    companion object {
        const val LOCK_INDEX_NAME = ".opensearch-alerting-config-lock"
        val LOCK_EXPIRED_MINUTES = TimeValue(5, TimeUnit.MINUTES)

        @JvmStatic
        fun lockMapping(): String? =
            LockService::class.java.classLoader
                .getResource("mappings/opensearch-alerting-config-lock.json")
                ?.readText()
    }

    fun lockIndexExist(): Boolean = clusterService.state().routingTable().hasIndex(LOCK_INDEX_NAME)

    fun acquireLock(
        scheduledJob: ScheduledJob,
        listener: ActionListener<LockModel?>,
    ) {
        val scheduledJobId = scheduledJob.id
        acquireLockWithId(scheduledJobId, listener)
    }

    fun acquireLockWithId(
        scheduledJobId: String,
        listener: ActionListener<LockModel?>,
    ) {
        val lockId = LockModel.generateLockId(scheduledJobId)
        createLockIndex(
            object : ActionListener<Boolean> {
                override fun onResponse(created: Boolean) {
                    if (created) {
                        try {
                            findLock(
                                lockId,
                                object : ActionListener<LockModel> {
                                    override fun onResponse(existingLock: LockModel?) {
                                        if (existingLock != null) {
                                            val currentTimestamp = getNow()
                                            if (isLockReleased(existingLock)) {
                                                log.debug("lock is released or expired: {}", existingLock)
                                                val updateLock = LockModel(existingLock, getNow(), false)
                                                updateLock(updateLock, listener)
                                            } else {
                                                log.debug("Lock is NOT released. {}", existingLock)
                                                if (existingLock.lockTime.epochSecond + LOCK_EXPIRED_MINUTES.seconds
                                                    < currentTimestamp.epochSecond
                                                ) {
                                                    log.debug("Lock is expired. Renewing Lock {}", existingLock)
                                                    val updateLock = LockModel(existingLock, getNow(), false)
                                                    updateLock(updateLock, listener)
                                                } else {
                                                    log.debug("Lock is NOT expired. Not running monitor {}", existingLock)
                                                    listener.onResponse(null)
                                                }
                                            }
                                        } else {
                                            val tempLock = LockModel(scheduledJobId, getNow(), false)
                                            log.debug("Lock does not exist. Creating new lock {}", tempLock)
                                            createLock(tempLock, listener)
                                        }
                                    }

                                    override fun onFailure(e: Exception) {
                                        listener.onFailure(e)
                                    }
                                },
                            )
                        } catch (e: VersionConflictEngineException) {
                            log.debug("could not acquire lock {}", e.message)
                            listener.onResponse(null)
                        }
                    } else {
                        listener.onResponse(null)
                    }
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            },
        )
    }

    private fun createLock(
        tempLock: LockModel,
        listener: ActionListener<LockModel?>,
    ) {
        try {
            val request =
                IndexRequest(LOCK_INDEX_NAME)
                    .id(tempLock.lockId)
                    .source(tempLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .setIfSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
                    .setIfPrimaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                    .create(true)
            client.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        listener.onResponse(LockModel(tempLock, response.seqNo, response.primaryTerm))
                    }

                    override fun onFailure(e: Exception) {
                        if (e is VersionConflictEngineException) {
                            log.debug("Lock is already created. {}", e.message)
                            listener.onResponse(null)
                            return
                        }
                        listener.onFailure(e)
                    }
                },
            )
        } catch (ex: IOException) {
            log.error("IOException occurred creating lock", ex)
            listener.onFailure(ex)
        }
    }

    private fun updateLock(
        updateLock: LockModel,
        listener: ActionListener<LockModel?>,
    ) {
        try {
            val updateRequest =
                UpdateRequest()
                    .index(LOCK_INDEX_NAME)
                    .id(updateLock.lockId)
                    .setIfSeqNo(updateLock.seqNo)
                    .setIfPrimaryTerm(updateLock.primaryTerm)
                    .doc(updateLock.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .fetchSource(true)

            client.update(
                updateRequest,
                object : ActionListener<UpdateResponse> {
                    override fun onResponse(response: UpdateResponse) {
                        listener.onResponse(LockModel(updateLock, response.seqNo, response.primaryTerm))
                    }

                    override fun onFailure(e: Exception) {
                        if (e is VersionConflictEngineException) {
                            log.debug("could not acquire lock {}", e.message)
                        }
                        if (e is DocumentMissingException) {
                            log.debug(
                                "Document is deleted. This happens if the job is already removed and" + " this is the last run." + "{}",
                                e.message,
                            )
                        }
                        if (e is IOException) {
                            log.error("IOException occurred updating lock.", e)
                        }
                        listener.onResponse(null)
                    }
                },
            )
        } catch (ex: IOException) {
            log.error("IOException occurred updating lock.", ex)
            listener.onResponse(null)
        }
    }

    fun findLock(
        lockId: String,
        listener: ActionListener<LockModel>,
    ) {
        val getRequest = GetRequest(LOCK_INDEX_NAME).id(lockId)
        client.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        listener.onResponse(null)
                    } else {
                        try {
                            val parser =
                                XContentType.JSON
                                    .xContent()
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.sourceAsString)
                            parser.nextToken()
                            listener.onResponse(LockModel.parse(parser, response.seqNo, response.primaryTerm))
                        } catch (e: IOException) {
                            log.error("IOException occurred finding lock", e)
                            listener.onResponse(null)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    log.error("Exception occurred finding lock", e)
                    listener.onFailure(e)
                }
            },
        )
    }

    fun release(
        lock: LockModel?,
        listener: ActionListener<Boolean>,
    ) {
        if (lock == null) {
            log.error("Lock is null. Nothing to release.")
            listener.onResponse(false)
        } else {
            log.debug("Releasing lock: {}", lock)
            val lockToRelease = LockModel(lock, true)
            updateLock(
                lockToRelease,
                object : ActionListener<LockModel?> {
                    override fun onResponse(releasedLock: LockModel?) {
                        listener.onResponse(releasedLock != null)
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                },
            )
        }
    }

    fun deleteLock(
        lockId: String,
        listener: ActionListener<Boolean>,
    ) {
        val deleteRequest = DeleteRequest(LOCK_INDEX_NAME).id(lockId)
        client.delete(
            deleteRequest,
            object : ActionListener<DeleteResponse> {
                override fun onResponse(response: DeleteResponse) {
                    listener.onResponse(
                        response.result == DocWriteResponse.Result.DELETED || response.result == DocWriteResponse.Result.NOT_FOUND,
                    )
                }

                override fun onFailure(e: Exception) {
                    if (e is IndexNotFoundException || e.cause is IndexNotFoundException) {
                        log.debug("Index is not found to delete lock. {}", e.message)
                        listener.onResponse(true)
                    } else {
                        listener.onFailure(e)
                    }
                }
            },
        )
    }

    private fun createLockIndex(listener: ActionListener<Boolean>) {
        if (lockIndexExist()) {
            listener.onResponse(true)
        } else {
            val indexRequest =
                CreateIndexRequest(LOCK_INDEX_NAME)
                    .mapping(lockMapping())
                    .settings(Settings.builder().put("index.hidden", true).build())
            client.admin().indices().create(
                indexRequest,
                object : ActionListener<CreateIndexResponse> {
                    override fun onResponse(response: CreateIndexResponse) {
                        listener.onResponse(response.isAcknowledged)
                    }

                    override fun onFailure(ex: Exception) {
                        log.error("Failed to update config index schema", ex)
                        if (ex is ResourceAlreadyExistsException || ex.cause is ResourceAlreadyExistsException) {
                            listener.onResponse(true)
                        } else {
                            listener.onFailure(ex)
                        }
                    }
                },
            )
        }
    }

    private fun isLockReleased(lock: LockModel): Boolean = lock.released

    private fun getNow(): Instant =
        if (testInstant != null) {
            testInstant!!
        } else {
            Instant.now()
        }

    fun setTime(testInstant: Instant) {
        this.testInstant = testInstant
    }
}
