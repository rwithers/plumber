package pl.touk.plk.rtm.load.nifi.flow.tester

import java.sql

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.DriverManager

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.persistence.TemplateDeserializer
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.standard.PutSQL
import org.apache.nifi.web.api.dto.TemplateDTO
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.io.Source

class IngestProcessorGroupFromApiSpec extends FunSuite with Matchers with BeforeAndAfter with IdiomaticMockito with ArgumentMatchersSugar {
  private var templateRef: TemplateDTO = null
  var service: DBCPService = null

  class PutSQLExtension extends PutSQL {
    var CONNECTION_POOL: PropertyDescriptor = null
    var SQL_STATEMENT: PropertyDescriptor = null
    var AUTO_COMMIT: PropertyDescriptor = null
    var SUPPORT_TRANSACTIONS: PropertyDescriptor = null
    var TRANSACTION_TIMEOUT: PropertyDescriptor = null
    var BATCH_SIZE: PropertyDescriptor = null
    var OBTAIN_GENERATED_KEYS: PropertyDescriptor = null
    var ROLLBACK_ON_FAILURE: PropertyDescriptor = null

    val descriptorList = this.getSupportedPropertyDescriptors

    for (i <- 0 until descriptorList.size()) {
      val d = descriptorList.get(i)

      if (d.getDisplayName == "JDBC Connection Pool") {
        CONNECTION_POOL = d
      } else if (d.getDisplayName == "SQL Statement") {
        SQL_STATEMENT = d
      } else if (d.getDisplayName == "Support Fragmented Transactions" ) {
        SUPPORT_TRANSACTIONS = d
      } else if (d.getDisplayName == "Database Session AutoCommit") {
        AUTO_COMMIT = d
      } else if (d.getDisplayName == "Transaction Timeout") {
        TRANSACTION_TIMEOUT = d
      } else if (d.getDisplayName == "Batch Size") {
        BATCH_SIZE = d
      } else if (d.getDisplayName == "Obtain Generated Keys") {
        OBTAIN_GENERATED_KEYS = d
      } else {
        // set roll back on failure
        ROLLBACK_ON_FAILURE = d
      }
    }

    println(CONNECTION_POOL);
    println(SQL_STATEMENT);
    println(AUTO_COMMIT);
    println(SUPPORT_TRANSACTIONS);
    println(TRANSACTION_TIMEOUT);
    println(BATCH_SIZE);
    println(OBTAIN_GENERATED_KEYS);
    println(ROLLBACK_ON_FAILURE);
  }

  before {
    setupInMemoryDatabase()
    templateRef = template("http://localhost:8080/nifi-api/templates/40c600fa-acb9-4346-844a-664cdde5b057/download");
  }

  test("Read Template and Enqueue Data") {
    val putSql = new PutSQLExtension()
    val flowbuilder = NifiTemplateFlowFactory(templateRef)

    flowbuilder.addControllerService(putSql.CONNECTION_POOL.getName, service, Map())
    val flow = flowbuilder.create()

    flow.getProcessGroup("Ingress")
  }

  private def template(url: String): TemplateDTO = {
    val str = scala.io.Source.fromURL(url).mkString
    val stream = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
    TemplateDeserializer.deserialize(stream)
  }

  // TODO: this method has side effects. It sets up an in memory database service. Refactoring is needed here to
  // take the sql file as a parameter and return a reference to the database service.
  private def setupInMemoryDatabase(): Unit = {
    service = new MockDBCPInMemory

    try {
      val bufferedSource = Source.fromFile("./src/test/resources/sql/job_tracker_master.sql")
      var sql:String = ""
      for (line <- bufferedSource.getLines) {
        sql = sql + line
      }
      bufferedSource.close

      val conn = service.getConnection
      try {
        val stmt = conn.createStatement
        try {
          println("executing sql: ", sql)
          stmt.execute(sql)
          println("sql create succeeded")
        } finally if (stmt != null) stmt.close()
      }
    }
    // we aren't closing the connection here because of the rules that h2 has for in memory databases.
    // for more information see: http://www.h2database.com/html/features.html?highlight=page&search=page#in_memory_databases
    // we deem this to be okay because this is a test fixture and everything will be torn down when the test is complete.
  }

  private class MockDBCPInMemory extends AbstractControllerService with DBCPService {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection("jdbc:h2:mem:agrho")

    override def getIdentifier = "dbcp"

    @throws[ProcessException]
    override def getConnection: sql.Connection = try {
      conn
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new ProcessException("getConnection failed: " + e)
    }
  }
}
