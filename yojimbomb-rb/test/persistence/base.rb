
#
# usage notes: require in this file,
# 	build whatever metricsKeeper you want to test,
# 	then call module method testPersistence w/ m.keeper as arg
#
#

require 'yojimbomb'
require 'securerandom'

module Yojimbomb
module Test
	
	HourAmt = 60 * 60
	DayAmt = HourAmt * 24

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
			:test, Time.now, Time.now + HourAmt, HourAmt/2,
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
		#create test data
		minTime = Time.now
		maxTime = Time.now + DayAmt
		times = [
			minTime,
			Time.now + (5 * HourAmt),
			maxTime
		]
		
		index = Hash.new {|h,k| h[k] = Array.new}
		metrics = []
		
		ptags = [] ; 3.times {|pt| ptags << "p#{pt}".to_sym }
		mtags = [] ; 3.times {|mt| mtags << "m#{mt}".to_sym }
		
		times.each do |time|
			ptags.each do |p1| ptags.each do |p2| mtags.each do |m1| mtags.each do |m2| 
				id = SecureRandom.uuid 
				index[nil] << id
				index[time] << id
				metrics << Yojimbomb::EventMetric.new(:test, time, :id => id, :primary => [p1,p2], :minor => [m1,m2] )
			end end end end
		end
		
		metricsKeeper.store(*metrics)
		
		#just occur	
		
		# everything
		critOccAll = Yojimbomb::Criteria.new(minTime - HourAmt/2, maxTime + HourAmt/2)
		res = metricsKeeper.find(:test, :event, critOccAll).map {|metric| metric.id}
		assertFalse(res.empty?)
		index[nil].each {|id| assertTrue(res.include?(id))}
		
		#ea. time group
		times.each do |time|
			critOcc = Yojimbomb::Criteria.new(time - HourAmt/2, time + HourAmt/2)
			res = metricsKeeper.find(:test, :event, critOcc).map {|metric| metric.id}
			assertFalse(res.empty?)
			times.each do |tx| index[tx].each do |id| case tx
				when time then assertTrue(res.include?(id))
				else assertFalse(res.include?(id))
			end end end
		end
		
		#tod
		tods = times.map {|time| Yojimbomb::DateTime.timeOfDay(time) }
		tods.uniq.each do |tod|
			lbound = tod < 1.0  ? 0.0 : tod - 1.0
			ubound = tod > 22.0 ? 23.0 : tod + 1.0
			todCrit = critOccMax = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:toStart => lbound, :todStop => ubound
			)
			res = metricsKeeper.find(:test,:event,todCrit)
			res.each do |metric|
				metricTod = metric.timeOfDay
				assertTrue(metricTod >= lbound)
				assertTrue(metricTod <= ubound)
			end			
		end
		
		#dow
		dows = times.map {|time| Yojimbomb::DateTime.dayOfWeek(time) } 
		dows.each do |dow|
			dowCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:dow => dow
			)
			res = metricsKeeper.find(:test,:event, dowCrit)
			res.each {|metric| assertTrue( metric.dayOfWeek == dow) }
		end
		
		#tags
		ptags.each do |p1| ptags.each do |p2|
		mtags.each do |m1| mtags.each do |m2|
			ptagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:primary => [p1,p2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.primaryTags.include?(p1))
				assertTrue(metric.primaryTags.include?(p2))
			end
			
			
			mtagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:minor => [m1,m2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.minorTags.include?(m1))
				assertTrue(metric.minorTags.include?(m2))
			end
			
			pmtagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:primary => [p1,p2].uniq, :minor => [m1,m2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.primaryTags.include?(p1))
				assertTrue(metric.primaryTags.include?(p2))
				assertTrue(metric.minorTags.include?(m1))
				assertTrue(metric.minorTags.include?(m2))
			end
			
		end end end end
		
	end

	#save & find periods
	def self.testFindPeriods(metricsKeeper)
		#create test data
		minTime = Time.now
		maxTime = Time.now + DayAmt
		times = [
			[minTime, minTime + (5 * HourAmt)],
			[Time.now + (10 * HourAmt), Time.now + (15 * HourAmt)],
			[maxTime, maxTime + (5 * HourAmt)]
		]
		
		index = Hash.new {|h,k| h[k] = Array.new}
		metrics = []
		
		ptags = [] ; 3.times {|pt| ptags << "p#{pt}".to_sym }
		mtags = [] ; 3.times {|mt| mtags << "m#{mt}".to_sym }
		
		times.each do |start, stop|
			ptags.each do |p1| ptags.each do |p2| mtags.each do |m1| mtags.each do |m2| 
				id = SecureRandom.uuid 
				index[nil] << id
				index[time] << id
				metrics << Yojimbomb::PeriodMetric.new(
					:test, start, stop, 30, 
					:id => id, :primary => [p1,p2], :minor => [m1,m2] 
				)
			end end end end
		end
		
		metricsKeeper.store(*metrics)
		
		#just occur	
		
		# everything
		critOccAll = Yojimbomb::Criteria.new(minTime - HourAmt/2, maxTime + HourAmt/2)
		res = metricsKeeper.find(:test, :period, critOccAll).map {|metric| metric.id}
		assertFalse(res.empty?)
		index[nil].each {|id| assertTrue(res.include?(id))}
		
		#ea. time group
		times.each do |time|
			critOcc = Yojimbomb::Criteria.new(time - HourAmt/2, time + HourAmt/2)
			res = metricsKeeper.find(:test, :period, critOcc).map {|metric| metric.id}
			assertFalse(res.empty?)
			times.each do |tx| index[tx].each do |id| case tx
				when time then assertTrue(res.include?(id))
				else assertFalse(res.include?(id))
			end end end
		end
		
		#tod
		tods = times.map {|time| Yojimbomb::DateTime.timeOfDay(time) }
		tods.uniq.each do |tod|
			lbound = tod < 1.0  ? 0.0 : tod - 1.0
			ubound = tod > 22.0 ? 23.0 : tod + 1.0
			todCrit = critOccMax = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:toStart => lbound, :todStop => ubound
			)
			res = metricsKeeper.find(:test,:period,todCrit)
			res.each do |metric|
				metricTodStart = metric.startTimeOfDay
				metricTodStop = metric.stopTimeOfDay
				matchTodStart = (metricTodStart >= lbound) && (metricTodStart <= ubound)
				matchTodStop = (metricTodStop >= lbound) && (metricTodStop <= ubound)
				assertTrue(matchTodStart || matchTodStop)
			end			
		end
		
		#dow
		dows = times.map {|time| Yojimbomb::DateTime.dayOfWeek(time) } 
		startNumDow = Yojimbomb::DateTime.numericWeekDay(Yojimbomb::DateTime::DaysOfWeek[0])
		endNumDow = Yojimbomb::DateTime.numericWeekDay(Yojimbomb::DateTime::DaysOfWeek[-1])
		dows.each do |dow|
			numDow = Yojimbomb::DateTime.numericWeekDay(dow)
			dowCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:dow => dow
			)
			res = metricsKeeper.find(:test,:period, dowCrit)
			res.each do |metric| 
				numStart = Yojimbomb::DateTime.numericWeekDay( metric.startDayOfWeek )
				numStop = Yojimbomb::DateTime.numericWeekDay( metric.stopDayOfWeek )
				
				range = if numStart < numStop
					(numStart..numStop).to_a
				else
					(numStart..endNumDow).to_a + (startNumDow..numStop).to_a
				end
				
				assertTrue(range.include?(numDow))
			end
		end
		
		#tags
		ptags.each do |p1| ptags.each do |p2|
		mtags.each do |m1| mtags.each do |m2|
			ptagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:primary => [p1,p2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.primaryTags.include?(p1))
				assertTrue(metric.primaryTags.include?(p2))
			end
			
			
			mtagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:minor => [m1,m2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.minorTags.include?(m1))
				assertTrue(metric.minorTags.include?(m2))
			end
			
			pmtagCrit = Yojimbomb::Criteria.new(
				minTime - HourAmt/2, maxTime + HourAmt/2,
				:primary => [p1,p2].uniq, :minor => [m1,m2].uniq
			)
			res = metricsKeeper.find(:test,:event, ptagCrit)
			res.each do |metric|
				assertTrue(metric.primaryTags.include?(p1))
				assertTrue(metric.primaryTags.include?(p2))
				assertTrue(metric.minorTags.include?(m1))
				assertTrue(metric.minorTags.include?(m2))
			end
			
		end end end end
	end
	
	def self.testReplaceEvent(metricsKeeper)
		#TODO	
	end
	
	def self.testReplacePeriod(metricsKeeper)
		#TODO
	end
end end
