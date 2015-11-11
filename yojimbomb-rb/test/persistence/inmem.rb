
require './base'
require 'yojimbomb/persistence/inmemory'

metricsKeeper = Yojimbomb::InMemMetricsKeeper.new
Yojimbomb::Test.testPersistence(metricsKeeper)
