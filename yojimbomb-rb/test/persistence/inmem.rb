
require './base'
require 'yojimbomb/persistence/inmemory'

metricsKeeper = Yojimbomb::InMemMetricsKeeper.new
metricsKeeper.logger = Logger.new(STDOUT)
Yojimbomb::Test.testPersistence(metricsKeeper)
