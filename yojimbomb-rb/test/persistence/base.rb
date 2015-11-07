
require 'yojimbomb'

module Yojimbomb
module Test

	def assertTrue(expr)
		throw :assertFailed unless expr
	end
	
	def assertFalse(expr)
		throw :assertFailed if expr
	end

	def isMetricIn?(metric, *metrics) 
		ids = metrics.map {|m| m.id}
		ids.include?(metric.id)
	end

	def self.testPersistence(metricsKeeper)
		self.testBasicEvent(metricsKeeper)
		self.testBasicPeriod(metricsKeeper)
	
		self.testFindEvents(metricsKeeper)
		self.testFindPeriods(metricsKeeper)
	
		self.testReplaceEvent(metricsKeeper)
		self.testReplacePeriod(metricsKeeper)
	
	end 

	#save 1 event, retrieve, delete
	def self.testBasicEvent(metricsKeeper)
		event = Yojimbomb::EventMetric.new(
			:test, Time.now,
			:quantity => 4,
			:count => 5,
			:primary => [:a,:b,:c],
			:minor => [:x,:y,:z]	
		)
		
		metricsKeeper.store(event)
		
		findRes = metricsKeepr.find(:test, :event)
		assertTrue( isMetricIn?(event, *findRes) )
		
		retEvent = nil
		findRes.each do |metric|
			next unless metric.id == event.id
			retEvent = metric
			break
		end
		
		assertTrue(retEvent.metricType == :test)
		assertTrue(retEvent.quantity == 4)
		assertTrue(retEvent.count == 5)
		assertTrue(retEvent.primaryTags.size == 3)
		event.primaryTags.each {|pt| assertTrue(retEvent.primaryTags.include?(pt))} 
		assertTrue(retEvent.minorTags.size == 3)
		event.minorTags.each {|mt| assertTrue(retEvent.minorTags.include?(mt))}
		assertTrue(event.occurence == retEvent.occurence)
		
		metricsKeeper.remove(:test, :event, event.id)
		
		findRes = findRes = metricsKeepr.find(:test, :event)
		assertFalse( isMetricIn?(event, *findRes))
	end

	#save 1 period, retrieve, delete
	def self.testBasicPeriod(metricsKeeper)
		period = Yojimbomb::PeriodMetric.new(
			:test, Time.now, Time.now + (60 * 60), 30,
			:quantity => 4,
			:count => 5,
			:primary => [:a,:b,:c],
			:minor => [:x,:y,:z]	
		)
		
		metricsKeeper.store(period)
		
		findRes = metricsKeepr.find(:test, :period)
		assertTrue( isMetricIn?(period, *findRes) )
		
		retPeriod = nil
		findRes.each do |metric|
			next unless metric.id == period.id
			retEvent = metric
			break
		end
		
		assertTrue(retPeriod.metricType == :test)
		assertTrue(retPeriod.count == 5)
		
		assertTrue(retPeriod.primaryTags.size == 3)
		period.primaryTags.each {|pt| assertTrue(retPeriod.primaryTags.include?(pt))} 
		assertTrue(retPeriod.minorTags.size == 3)
		period.minorTags.each {|mt| assertTrue(retPeriod.minorTags.include?(mt))}
		
		assertTrue(period.start == retPeriod.start)
		assertTrue(period.stop == retPeriod.stop)
		
		assertTrue(period.duration == retPeriod.duration)
		assertTrue(period.startDayOfWeek == retPeriod.startDayOfWeek)
		assertTrue(period.stopDayOfWeek == retPeriod.stopDayOfWeek)
		assertTrue(period.startTimeOfDay == retPeriod.startTimeOfDay)
		assertTrue(period.stopTimeOfDay == retPeriod.stopTimeOfDay)
		
		metricsKeeper.remove(:test, :period, period.id)
		
		findRes = findRes = metricsKeepr.find(:test, :period)
		assertFalse( isMetricIn?(period, *findRes) )
	end
	
	#save & find events
	def self.testFindEvents(metricsKeeper)
		#TODO create test data
		
		#TODO just occur
		
		#TODO tod
		
		#TODO dow
		
		#TODO primary tags
		
		#TODO minor tags
		
		#TODO everything
	end

	#save & find periods
	def self.testFindPeriods(metricsKeeper)
		#TODO create test data
		
		#TODO just occur
		
		#TODO tod
		
		#TODO dow
		
		#TODO primary tags
		
		#TODO minor tags
		
		#TODO everything
		
	end
	
	def self.testReplaceEvent(metricsKeeper)
		#TODO	
	end
	
	def self.testReplacePeriod(metricsKeeper)
		#TODO
	end
end end

