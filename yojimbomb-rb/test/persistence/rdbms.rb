
require './base'
require 'yojimbomb/persistence/rdbms'

#$DBConnect = $*[0]
$DBConnect = {
	:adapter => 'mysql2',
	:host => 'localhost',
	:database => 'yojimbomb_test',
	:user => 'ybtest',
	:password => 'password',	
}

metricsKeeper = Yojimbomb::RdbmsMetricsKeeper.connect($DBConnect)
metricsKeeper.logger = Logger.new(STDOUT)
Yojimbomb::Test.testPersistence(metricsKeeper)
