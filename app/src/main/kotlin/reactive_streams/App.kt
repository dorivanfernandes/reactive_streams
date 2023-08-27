package reactive_streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import java.lang.Thread.sleep
import kotlin.random.Random

suspend fun main() {
//    blocking()
    nonBlocking()
}

fun blocking() {
    val query = Database()

    val result = query.executeQuery("SELECT * FROM products order by id asc;")

    val chunkSize = 10000
    val values = mutableListOf<String>()
    while (result.next()) {
        sleep(2)
        values.add("UPDATE products set price = ${Random.nextDouble(10.00, 10000.00)} where id = ${result.getInt("id")};")

        if (values.size >= chunkSize) {
            println("Updating ${values.size} rows")
            query.insertBatch(values)
            values.clear()
        }
    }
}

suspend fun nonBlocking() {
    val query = Database()

    val result = query.executeQuery("SELECT * FROM products order by id asc;")

    val chunkSize = 10000
    val values = mutableListOf<String>()
    while (result.next()) {
            flow {
                delay(2)
                values.add("UPDATE products set price = ${Random.nextDouble(10.00, 10000.00)} where id = ${result.getInt("id")};")

                if (values.size >= chunkSize) {
                    emit(values)
                    values.clear()
                }
            }.collect {
                println("Updating ${values.size} rows")
                query.insertBatch(values)
            }
    }
}