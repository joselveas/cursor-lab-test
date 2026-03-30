package com.spsgroup.argos.feature.patrol.data.repository

import com.couchbase.lite.Database
import com.couchbase.lite.MutableDocument
import com.couchbase.lite.QueryBuilder
import com.couchbase.lite.SelectResult
import com.couchbase.lite.DataSource
import com.couchbase.lite.Expression
import com.couchbase.lite.Meta
import com.couchbase.lite.Dictionary
import com.couchbase.lite.Document
import com.couchbase.lite.Result as CbResult
import com.spsgroup.argos.core.data.local.util.toCouchbaseMap
import com.spsgroup.argos.feature.patrol.data.remote.PatrolApi
import com.spsgroup.argos.feature.patrol.domain.model.Binnacle
import com.spsgroup.argos.feature.patrol.domain.model.MaritimeContext
import com.spsgroup.argos.feature.patrol.domain.model.MediaAttachment
import com.spsgroup.argos.feature.patrol.domain.model.MediaType
import com.spsgroup.argos.feature.patrol.domain.model.Patrol
import com.spsgroup.argos.feature.patrol.domain.model.RiskLevel
import com.spsgroup.argos.feature.patrol.domain.model.ScheduleType
import com.spsgroup.argos.feature.patrol.domain.model.Telemetry
import com.spsgroup.argos.feature.patrol.domain.repository.PatrolRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.json.JSONObject
import java.util.UUID
import javax.inject.Inject

class PatrolRepositoryImpl @Inject constructor(
    private val database: Database,
    private val api: PatrolApi,
    private val networkJson: Json
) : PatrolRepository {
    private inline fun <reified T> Dictionary.toDomainObject(): T {
        val jsonStr = JSONObject(this.toMap()).toString()
        return networkJson.decodeFromString<T>(jsonStr)
    }
    private inline fun <reified T> Document.toDomainObject(): T {
        val jsonStr = JSONObject(this.toMap()).toString()
        return networkJson.decodeFromString<T>(jsonStr)
    }
    override fun getPatrols(): Flow<List<Patrol>> = callbackFlow {
        val query = QueryBuilder.select(SelectResult.all())
            .from(DataSource.database(database))
            .where(Expression.property("documentType").equalTo(Expression.string("patrol")))
        val listenerToken = query.addChangeListener { change ->
            val rawResults = change.results?.allResults() ?: emptyList()
            android.util.Log.d("ARGOS_DB", "Documentos crudos encontrados: ${rawResults.size}")
            val results = rawResults.mapNotNull { result ->
                try {
                    result.getDictionary(database.name)?.toDomainObject<Patrol>()
                } catch (e: Exception) {
                    android.util.Log.e("ARGOS_DB", "Error de parseo JSON a Domain: ${e.message}")
                    null
                }
            }
            android.util.Log.d("ARGOS_DB", "Documentos emitidos a la UI: ${results.size}")
            trySend(results)
        }
        awaitClose {
            query.removeChangeListener(listenerToken)
        }
    }.flowOn(Dispatchers.IO)
    override suspend fun getMaritimeContext(patrolId: String): Result<MaritimeContext> = withContext(Dispatchers.IO) {
        try {
            val docId = "context::$patrolId"
            val document = database.getDocument(docId)
                ?: throw IllegalStateException("Carta de Situación no encontrada localmente para el ID: $patrolId")
            val context = document.toDomainObject<MaritimeContext>()
            Result.success(context)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun populateWithDummies(): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val checkQuery = QueryBuilder.select(SelectResult.expression(Meta.id))
                .from(DataSource.database(database))
                .where(Expression.property("documentType").equalTo(Expression.string("patrol")))
                .limit(Expression.intValue(1))
            val hasData = checkQuery.execute().allResults().isNotEmpty()
            if (hasData) {
                android.util.Log.d("ARGOS_DB", "DB ya contiene datos. Omitiendo población de dummies.")
                return@withContext Result.success(Unit)
            }
            android.util.Log.d("ARGOS_DB", "DB vacía. Inyectando patrullajes de prueba...")
            database.inBatch<Exception> {
                val dummyPatrols = listOf(
                    Patrol(
                        id = UUID.randomUUID().toString(),
                        identifier = "TURNO-MAÑANA-01",
                        type = 1L,
                        contextAccepted = false,
                        syncStatus = "SYNCED",
                        latitudeStart = -29.959331,
                        longitudeStart = -71.33422
                    ),
                    Patrol(
                        id = UUID.randomUUID().toString(),
                        identifier = "TURNO-TARDE-02",
                        type = 2L,
                        contextAccepted = false,
                        syncStatus = "SYNCED",
                        latitudeStart = -29.960000,
                        longitudeStart = -71.335000
                    )
                )
                dummyPatrols.forEach { patrol ->
                    val patrolDoc = MutableDocument("patrol::${patrol.id}", patrol.toCouchbaseMap())
                    database.save(patrolDoc)
                    val maritimeContext = MaritimeContext(
                        patrolId = patrol.id,
                        crimeAnalysis = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.CrimeAnalysis(
                                type = com.spsgroup.argos.feature.patrol.domain.model.CrimeType.PESCA_ILEGAL,
                                profile = com.spsgroup.argos.feature.patrol.domain.model.CrimeProfile(
                                    victim = "Sindicato de Pescadores Artesanales",
                                    suspect = "Embarcaciones no identificadas (NN)"
                                ),
                                modusOperandi = "Extracción de recursos bentónicos en área de manejo durante horario nocturno, utilizando equipos de buceo semi-autónomo y operando sin luces de navegación."
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.CrimeAnalysis(
                                type = com.spsgroup.argos.feature.patrol.domain.model.CrimeType.ROBO_LUGAR_HABITADO, // Asumiendo que existe este Enum
                                profile = com.spsgroup.argos.feature.patrol.domain.model.CrimeProfile(
                                    victim = "Instalaciones Portuarias / Almacenes",
                                    suspect = "Bandas organizadas locales"
                                ),
                                modusOperandi = "Ingreso por la fuerza a recintos extraportuarios durante la madrugada, evadiendo rondas de seguridad privada para sustraer carga de alto valor comercial."
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.CrimeAnalysis(
                                type = com.spsgroup.argos.feature.patrol.domain.model.CrimeType.CONTRABANDO, // Asumiendo que existe este Enum
                                profile = com.spsgroup.argos.feature.patrol.domain.model.CrimeProfile(
                                    victim = "Fisco / Estado de Chile",
                                    suspect = "Tripulantes de buques mercantes"
                                ),
                                modusOperandi = "Ocultamiento de mercancía no declarada (cigarrillos y tecnología) en compartimentos falsos cercanos a la sala de máquinas del buque."
                            )
                        ),
                        crimeConcentrations = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.CrimeConcentration(
                                type = com.spsgroup.argos.feature.patrol.domain.model.CrimeType.PESCA_ILEGAL,
                                startTime = "01:00",
                                endTime = "04:30",
                                location = "Área Sur Bahía de Coquimbo, cuadrante 4"
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.CrimeConcentration(
                                type = com.spsgroup.argos.feature.patrol.domain.model.CrimeType.ROBO_LUGAR_HABITADO,
                                startTime = "02:00",
                                endTime = "06:00",
                                location = "Sector de acopio portuario norte, andenes 3 y 4"
                            )
                        ),
                        specificTasks = com.spsgroup.argos.feature.patrol.domain.model.SpecificTasks(
                            prevention = listOf(
                                com.spsgroup.argos.feature.patrol.domain.model.PreventionTask(
                                    task = "Control de zarpe y recalada en muelles no autorizados",
                                    procedure = "Verificación de documentación marítima y elementos de seguridad a bordo según pauta PM-1."
                                ),
                                com.spsgroup.argos.feature.patrol.domain.model.PreventionTask(
                                    task = "Control de zarpe y recalada en muelles no autorizados",
                                    procedure = "Verificación de documentación marítima y elementos de seguridad a bordo según pauta PM-1."
                                ),
                                com.spsgroup.argos.feature.patrol.domain.model.PreventionTask(
                                    task = "Control de zarpe y recalada en muelles no autorizados",
                                    procedure = "Verificación de documentación marítima y elementos de seguridad a bordo según pauta PM-1."
                                )
                            ),
                            procedures = listOf(
                                "Notificación por infracción a la Ley General de Pesca y Acuicultura (Ley 18.892).",
                                "Control de identidad preventivo (Art. 85 CPP) en sectores de acopio."
                            ),
                            judicialOrders = listOf(
                                "Orden de investigar Fiscalía Local Coquimbo RUC 2400123456-7",
                                "Orden de investigar Fiscalía Local Coquimbo RUC 2400123456-7",
                                "Orden de investigar Fiscalía Local Coquimbo RUC 2400123456-7"
                            ),
                            maritimeInspections = listOf(
                                com.spsgroup.argos.feature.patrol.domain.model.MaritimeInspectionTask(
                                    laborType = "Fiscalización de Cuotas",
                                    startTime = "08:00",
                                    endTime = "12:00",
                                    detail = "Revisión de cuotas de desembarque de merluza común.",
                                    isCompliant = false,
                                    procedure = "Decomiso de artes de pesca y citación a tribunal marítimo.",
                                    signature = null
                                )
                            )
                        ),
                        incidentVessels = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.IncidentVessel(
                                photo = "imagen1",
                                description = "Lancha a motor 'Don Francisco II', matrícula CB-1234. Reportada por evasión de control naval la semana anterior."
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.IncidentVessel(
                                photo = "imagen2",
                                description = "Bote artesanal 'Mar Brava', sin matrícula visible. Interceptado previamente por extracción ilegal de recursos bentónicos."
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.IncidentVessel(
                                photo = "imagen3",
                                description = "Embarcación menor 'La Consentida'. Modificación estructural no autorizada para carga pesada."
                            )
                        ),
                        documentType = "maritimeContext"
                    )
                    val contextDoc = MutableDocument("context::${patrol.id}", maritimeContext.toCouchbaseMap())
                    database.save(contextDoc)
                    val binnacle = com.spsgroup.argos.feature.patrol.domain.model.Binnacle(
                        id = "logbook::${patrol.id}",
                        patrolId = patrol.id,
                        documentType = "logbook",
                        scheduleType = if (patrol.identifier.contains("MAÑANA")) ScheduleType.DAYTIME else ScheduleType.NIGHTTIME_PM,
                        createdAt = System.currentTimeMillis(),
                        riskLevel = if (patrol.identifier.contains("TARDE")) RiskLevel.SERIOUS else RiskLevel.MODERATE,
                        task = "Patrullaje preventivo de borde costero",
                        patrolMembers = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.Personnel(
                                id = UUID.randomUUID().toString(),
                                rank = "Sargento 1ro",
                                firstName = "Juan",
                                lastName = "Astudillo",
                                npi = "15345678-9",
                                weaponType = "Pistola 9mm",
                                weaponSerialNumber = "WP-99231"
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.Personnel(
                                id = UUID.randomUUID().toString(),
                                rank = "Cabo 2do",
                                firstName = "José",
                                lastName = "Veas",
                                npi = "19876543-2",
                                weaponType = "Ninguna",
                                weaponSerialNumber = "N/A"
                            )
                        ),
                        vehicle = com.spsgroup.argos.feature.patrol.domain.model.Vehicle(
                            id = UUID.randomUUID().toString(),
                            type = com.spsgroup.argos.feature.patrol.domain.model.VehicleType.OTHER,
                            description = "Camioneta 4x4 Institucional",
                            identification = "PM-2504"
                        ),
                        inspectors = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.Inspector(
                                id = UUID.randomUUID().toString(),
                                rank = "Inspector Sernapesca",
                                firstName = "Carlos",
                                lastName = "Pérez",
                                participationTime = System.currentTimeMillis() - 3600000,
                                participationLocation = "Muelle Pesquero Coquimbo",
                                observations = "Control de cuota de pesca. Sin infracciones.",
                                hasSignature = true
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.Inspector(
                                id = UUID.randomUUID().toString(),
                                rank = "Fiscalizador Aduanas",
                                firstName = "María",
                                lastName = "González",
                                participationTime = System.currentTimeMillis() - 1800000,
                                participationLocation = "Puerto Seco",
                                observations = "Revisión de manifiesto de carga. Todo en regla.",
                                hasSignature = false
                            )
                        ),
                        procedures = listOf(
                            com.spsgroup.argos.feature.patrol.domain.model.Procedure(
                                id = UUID.randomUUID().toString(),
                                reason = "Consumo de alcohol en vía pública",
                                startTime = System.currentTimeMillis() - 7200000, // Hace 2 horas
                                endTime = System.currentTimeMillis() - 5400000,  // Duró 30 min
                                location = com.spsgroup.argos.feature.patrol.domain.model.Place(
                                    name = "Sector Playa Norte",
                                    street = "Av. Costanera",
                                    number = 450,
                                    commune = "Coquimbo",
                                    latitude = -29.948,
                                    longitude = -71.328
                                ),
                                events = "Se sorprende a grupo de 3 personas consumiendo bebidas alcohólicas. Se procede a cursar infracción y retiro del lugar.",
                                terminationType = com.spsgroup.argos.feature.patrol.domain.model.TerminationType.INFRACTION,
                                citationCount = 1,
                                detaineeCount = com.spsgroup.argos.feature.patrol.domain.model.DetaineeCount(
                                    women = 1,
                                    men = 2,
                                    minors = 0
                                ),
                                identityCheckCount = 3,
                                vesselCount = 0,
                                attachments = listOf(
                                    MediaAttachment(
                                        id = "M1",
                                        localUri = "android.resource://com.spsgroup.argos/drawable/proc_photo_1",
                                        type = MediaType.PHOTO
                                    ),
                                    MediaAttachment(id = "M2", localUri = "android.resource://com.spsgroup.argos/drawable/proc_photo_2", type = MediaType.PHOTO),
                                    MediaAttachment(id = "M3", localUri = "android.resource://com.spsgroup.argos/raw/proc_video_1", type = MediaType.VIDEO)
                                )
                            ),
                            com.spsgroup.argos.feature.patrol.domain.model.Procedure(
                                id = UUID.randomUUID().toString(),
                                reason = "Pesca ilegal detectada",
                                startTime = System.currentTimeMillis() - 3600000, // Hace 1 hora
                                endTime = null, // Procedimiento en curso
                                location = com.spsgroup.argos.feature.patrol.domain.model.Place(
                                    name = "Caleta San Pedro",
                                    street = "Sector Rocas",
                                    number = 0,
                                    commune = "La Serena",
                                    latitude = -29.890,
                                    longitude = -71.285
                                ),
                                events = "Avistamiento de embarcación sin matrícula realizando extracción en zona de manejo. Personal en trayecto para intercepción.",
                                terminationType = com.spsgroup.argos.feature.patrol.domain.model.TerminationType.INSPECTION,
                                citationCount = 0,
                                detaineeCount = com.spsgroup.argos.feature.patrol.domain.model.DetaineeCount(
                                    women = 0,
                                    men = 0,
                                    minors = 0
                                ),
                                identityCheckCount = 0,
                                vesselCount = 1,
                                attachments = listOf(
                                    MediaAttachment(id = "M4", localUri = "android.resource://com.spsgroup.argos/drawable/proc_photo_3", type = MediaType.PHOTO),
                                    MediaAttachment(id = "M5", localUri = "android.resource://com.spsgroup.argos/drawable/proc_photo_4", type = MediaType.PHOTO)
                                )
                            )
                        ),
                        syncStatus = com.spsgroup.argos.feature.patrol.domain.model.SyncStatus.SYNCED
                    )
                    val binnacleDoc = MutableDocument(binnacle.id, binnacle.toCouchbaseMap())
                    database.save(binnacleDoc)
                }
            }
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun acceptMaritimeContext(patrolId: String): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val patrolDocId = "patrol::$patrolId"

            database.inBatch<Exception> {
                val document = database.getDocument(patrolDocId)
                    ?: throw Exception("Patrol no encontrado")
                val patrol = document.toDomainObject<Patrol>()
                val currentTime = System.currentTimeMillis()
                val updatedPatrol = patrol.copy(
                    contextAccepted = true,
                    startDateTime = currentTime,
                    syncStatus = "PENDING"
                )
                val patrolMap = updatedPatrol.toCouchbaseMap().toMutableMap()
                patrolMap["isActive"] = true
                val mutablePatrolDoc = document.toMutable()
                mutablePatrolDoc.setData(patrolMap)
                database.save(mutablePatrolDoc)
            }
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun getActivePatrol(): Result<Patrol?> = withContext(Dispatchers.IO) {
        try {
            val query = QueryBuilder.select(SelectResult.all())
                .from(DataSource.database(database))
                .where(
                    Expression.property("documentType").equalTo(Expression.string("patrol"))
                        .and(Expression.property("isActive").equalTo(Expression.booleanValue(true)))
                )
                .limit(Expression.intValue(1))
            var activePatrol: Patrol? = null
            query.execute().use { resultSet ->
                val result = resultSet.allResults().firstOrNull()
                if (result != null) {
                    activePatrol = result.getDictionary(database.name)?.toDomainObject<Patrol>()
                }
            }
            Result.success(activePatrol)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun getPatrolById(id: String): Result<Patrol?> = withContext(Dispatchers.IO) {
        try {
            val document = database.getDocument("patrol::$id")
            var patrol: Patrol? = null
            if (document != null) {
                patrol = document.toDomainObject<Patrol>()
            }
            Result.success(patrol)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun saveTelemetry(telemetry: Telemetry): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val docId = "telemetry::${telemetry.patrolId}::${telemetry.timestamp}"
            val doc = MutableDocument(docId, telemetry.toCouchbaseMap())
            database.save(doc)
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override fun observeTelemetry(patrolId: String): Flow<List<Telemetry>> = callbackFlow {
        val query = QueryBuilder.select(SelectResult.all())
            .from(DataSource.database(database))
            .where(
                Expression.property("documentType").equalTo(Expression.string("telemetry"))
                    .and(Expression.property("patrolId").equalTo(Expression.string(patrolId)))
            )
            .orderBy(com.couchbase.lite.Ordering.property("timestamp").ascending())
        val token = query.addChangeListener { change ->
            val results = change.results?.allResults()?.mapNotNull { result ->
                try {
                    result.getDictionary(database.name)?.toDomainObject<Telemetry>()
                } catch (e: Exception) { null }
            } ?: emptyList()

            trySend(results)
        }
        awaitClose { query.removeChangeListener(token) }
    }.flowOn(Dispatchers.IO)
    override suspend fun getBinnacle(patrolId: String): Result<Binnacle?> = withContext(Dispatchers.IO) {
        try {
            val docId = "logbook::$patrolId"
            val document = database.getDocument(docId)

            var binnacle: Binnacle? = null
            if (document != null) {
                binnacle = document.toDomainObject<Binnacle>()
            }
            Result.success(binnacle)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun saveBinnacle(binnacle: Binnacle): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val document = MutableDocument(binnacle.id, binnacle.toCouchbaseMap())
            database.save(document)
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
    override suspend fun finishPatrol(patrolId: String): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            val docId = "patrol::$patrolId"
            database.inBatch<Exception> {
                val document = database.getDocument(docId)
                    ?: throw Exception("Patrol no encontrado para finalizar")
                val patrolMap = document.toMap().toMutableMap()
                patrolMap["isActive"] = false
                patrolMap["endDateTime"] = System.currentTimeMillis()
                val mutableDoc = document.toMutable()
                mutableDoc.setData(patrolMap)
                database.save(mutableDoc)
            }
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(e)
        }
    }
}