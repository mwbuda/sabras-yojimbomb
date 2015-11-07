
require 'base'
require 'yojimbomb/persistence/rdbms/sequel'

$DBConnect = $*[0]

metricsKeeper = Yojimbomb::RDBMS::SequelMetricsKeeper.new($DBConnect)
Yojimbomb::Test.testPersistence(metricsKeeper)
