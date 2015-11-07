
require 'base'
require 'yojimbomb/persistence/inmemory'

metricsKeeper = InMemMetricsKeeper.new
Yojimbomb::Test.testPersistence(metricsKeeper)
