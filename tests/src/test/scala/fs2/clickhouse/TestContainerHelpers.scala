package fs2.clickhouse

import com.dimafeng.testcontainers.ClickHouseContainer

trait TestContainerHelpers {
  
  def httpApiPort(implicit clickHouseContainer: ClickHouseContainer): Int =
    clickHouseContainer.mappedPort(8123)

  def username(implicit clickHouseContainer: ClickHouseContainer): String =
    clickHouseContainer.username

  def password(implicit clickHouseContainer: ClickHouseContainer): String =
    clickHouseContainer.password

  def hostName(implicit clickHouseContainer: ClickHouseContainer): String =
    clickHouseContainer.host

}
