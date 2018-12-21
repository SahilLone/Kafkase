package com.kafka.admin

package com.applift.commons.kafka

import com.applift.commons.ext.debug
import com.applift.commons.ext.error
import com.applift.commons.ext.log
import com.applift.commons.ext.rxjava.toSingle
import com.applift.commons.ext.withThrowable
import io.reactivex.Completable
import io.reactivex.Single
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import java.util.Properties

sealed class MigrationResult
object MigrationResultSuccess : MigrationResult()
data class MigrationResultFailed(val failedTopics: List<KafkaOperations.TopicInfo>) : MigrationResult()

object KafkaOperations {

    val logger = log

    enum class ValidationResultField {
        NO_OF_PARTITIONS,
        REPLICATION_FACTOR
    }

    data class Broker(val bootstrapServers: String) {
        private val servers by lazy {
            bootstrapServers.split(",").toList()
        }

        val client: AdminClient by lazy {
            val clientProps = Properties()
            clientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, servers.joinToString(separator = ","))
            AdminClient.create(clientProps)
        }

        /*
        * Returns the cluster information containing topic info
        */
        fun currentTopics(): Single<Set<TopicInfo>> =
            client.listTopics().names().toSingle().flatMap { topicName ->
                client.describeTopics(topicName).all().toSingle().map { topics ->
                    topics.keys.map { topicName ->
                        val description = topics.get(topicName)
                        val partitions = description?.partitions()?.size ?: -1
                        val replicas = description?.partitions()?.map { it.replicas().size }?.max() ?: -1
                        TopicInfo(topicName, NoOfPartitions(partitions), ReplicationFactor(replicas))
                    }.toSet()
                }
            }.onErrorResumeNext {
                logger.error { "Error while getting topic information from broker" withThrowable it }
                Single.error(RuntimeException(it))
            }
    }

    data class TopicInfo(val name: String, val noOfPartitions: NoOfPartitions, val replicationFactor: ReplicationFactor) {
        override fun equals(other: Any?) =
            when (other) {
                is TopicInfo -> this.name == other.name
                else -> false
            }

        override fun hashCode(): Int {
            return this.name.hashCode()
        }
    }

    data class NoOfPartitions(val value: Int)
    data class ReplicationFactor(val value: Int)

    data class ValidationResultEntry<T>(val topicInfo: TopicInfo, val field: ValidationResultField, val expectedValue: T, val currentValue: T)
    data class ValidationResult<T>(val result: List<ValidationResultEntry<T>> = listOf()) {
        fun add(entries: List<ValidationResultEntry<T>>): ValidationResult<T> =
            ValidationResult(result.plus(entries))
    }

    data class KafkaMigrateCommand(private val broker: Broker, private val topics: Set<TopicInfo>) {

        private fun validate(): Single<ValidationResult<Int>> =
            broker.currentTopics().map { currentTopics ->
                topics.map { topic ->
                    if (currentTopics.contains(topic)) {
                        val currentProperties = currentTopics.find { it.name == topic.name }!!
                        val currentReplicationFactor = currentProperties.replicationFactor.value
                        val currentNoOfPartitions = currentProperties.noOfPartitions.value
                        ValidationResult<Int>(listOf()).add(
                            if (currentNoOfPartitions != topic.noOfPartitions.value) {
                                listOf(ValidationResultEntry(topic, ValidationResultField.NO_OF_PARTITIONS, topic.noOfPartitions.value, currentNoOfPartitions))
                            } else {
                                listOf()
                            }

                        ).add(
                            if (currentReplicationFactor != topic.replicationFactor.value) {
                                listOf(ValidationResultEntry(topic, ValidationResultField.NO_OF_PARTITIONS, topic.replicationFactor.value, currentReplicationFactor))
                            } else {
                                listOf()
                            }
                        )
                    } else {
                        ValidationResult<Int>(listOf()).add(
                            listOf(ValidationResultEntry(topic, ValidationResultField.NO_OF_PARTITIONS, topic.noOfPartitions.value, -1))
                        ).add(
                            listOf(ValidationResultEntry(topic, ValidationResultField.REPLICATION_FACTOR, topic.replicationFactor.value, -1))
                        )
                    }

                }
            }.map { it.flatten() }

        private fun updateTopics(): Single<ValidationResult<Int>> =
            validate().flatMap { validationResult ->
                val newTopicsToModifiedTopics = validationResult.result.partition {
                    it.currentValue == -1
                }
                val createOptions = CreateTopicsOptions().validateOnly(false)
                val createFuture = broker.client.createTopics(newTopicsToModifiedTopics.first.map {
                    NewTopic(it.topicInfo.name, it.topicInfo.noOfPartitions.value, it.topicInfo.replicationFactor.value.toShort())
                }, createOptions).all()
                val modifiedPartitionTopics = newTopicsToModifiedTopics.second.filter {
                    it.field == ValidationResultField.NO_OF_PARTITIONS
                }.fold(mapOf<String, NewPartitions>()) { agg, changedPartitionTopic ->
                    agg.plus(changedPartitionTopic.topicInfo.name to NewPartitions.increaseTo(changedPartitionTopic.expectedValue))
                }
                val modifyFuture = broker.client.createPartitions(modifiedPartitionTopics).all()
                Completable.fromFuture(KafkaFuture.allOf(createFuture, modifyFuture)).toSingle { validationResult }
            }.doOnError {
                logger.error { "Failed to apply kafka topic changes" withThrowable it }
            }.doAfterSuccess {
                logger.debug { "Successfully applied changes for topics" }
            }

        fun migrateChanges(): Single<MigrationResult> =
            updateTopics().flatMap {
                validate()
            }.map {
                if (it.result.isEmpty()) {
                    MigrationResultSuccess
                } else {
                    MigrationResultFailed(it.result.map { it.topicInfo })
                }
            }
    }

    @JvmStatic
    fun main(args: Array<String>) {

        val broker = Broker("localhost:9092")
        val command = KafkaMigrateCommand(broker,
            setOf(TopicInfo("ds14", NoOfPartitions(5), ReplicationFactor(1)),
                TopicInfo("ds13", NoOfPartitions(5), ReplicationFactor(1))))
        command.migrateChanges()
            .subscribe({
                when (it) {
                    is MigrationResultSuccess -> println("passed")
                    is MigrationResultFailed -> println(it.failedTopics.pretty())
                }
            }, {
                println("shit failed")
                throw it
            })

        broker.currentTopics().subscribe({
            println(it.toList().pretty())
        }, {
            it.printStackTrace()
        })

    }
}

fun <T, U, S> Single<T>.innerMap(mapper: (U) -> S): Single<List<S>> where T : Iterable<U> =
    this.map {
        it.map(mapper)
    }

fun <T> List<KafkaOperations.ValidationResult<T>>.flatten(): KafkaOperations.ValidationResult<T> =
    this.fold(KafkaOperations.ValidationResult()) { agg, vr ->
        agg.add(vr.result)
    }

fun List<KafkaOperations.TopicInfo>.pretty(): String {
    val builder = StringBuilder()
    builder.append("Topics Information\n")
    builder.append(
        this.joinToString(separator = "\n") {
            "Name: ${it.name} -> Partitions: ${it.noOfPartitions.value} -> " +
                    "ReplicationFactor:${it.replicationFactor.value}"
        }
    )
    return builder.toString()
}
