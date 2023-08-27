package reactive_streams

import kotlinx.coroutines.flow.flow
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.random.Random

class Database {
    private val url = "jdbc:postgresql://localhost:5432/reactive"

    fun connection(): Connection =
        DriverManager.getConnection(url, "postgres", "postgres")


    fun executeQuery(query: String): ResultSet =
        connection()
            .createStatement()
            .executeQuery(query)

    fun executeUpdate(query: String): Int =
        connection()
            .createStatement()
            .executeUpdate(query)

    fun insertBatch(values: List<String>) {
        val statement = connection().createStatement()
        values.forEach {
            statement.addBatch(it)
        }
        statement.executeBatch()
    }
}

suspend fun insertNonBlocking() {
    val query = Database()

    query.executeUpdate("CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name VARCHAR(255), price MONEY);")

    flow {
        val chunkSize = 10000
        val values = mutableListOf<String>()
        (1..5000000).forEach {
            val value = Random.nextDouble(10.00, 10000.00)
            values.add("INSERT INTO products (name, price) VALUES ('Product $it', $value);")

            if (values.size >= chunkSize) {
                emit(values)
                values.clear()
            }
        }

        emit(values)
    }.collect {
        println(it.size)
        query.insertBatch(it)
    }
}

fun insertBlocking() {
    val query = Database()

    query.executeUpdate("CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name VARCHAR(255), price MONEY);")

    val chunkSize = 10000
    val values = mutableListOf<String>()
    (1..5000000).forEach {
        val value = Random.nextDouble(10.00, 10000.00)
        values.add("INSERT INTO products (name, price) VALUES ('Product $it', $value);")

        if (values.size >= chunkSize) {
            println(values.size)
            query.insertBatch(values)
            values.clear()
        }
    }
}

suspend fun main() {
    insertNonBlocking()
//    insertBlocking()
}