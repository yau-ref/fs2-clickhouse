package fs2.clickhouse

import cats.effect.{Resource, SyncIO}
import com.dimafeng.testcontainers.ClickHouseContainer

import java.sql.{Connection, DriverManager}

trait WithConnection { self: TestContainerHelpers =>

  def withConnection[T](action: Connection => T)(implicit clickHouseContainer: ClickHouseContainer): T =
    Resource.fromAutoCloseable(
      SyncIO.delay(
        DriverManager
          .getConnection(
            clickHouseContainer.container.getJdbcUrl,
            username,
            password
          )
      )
    ).use(connection => SyncIO.delay(action(connection))).unsafeRunSync()
  
}

