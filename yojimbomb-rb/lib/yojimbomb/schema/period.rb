
require 'yojimbomb/schema/base'

module Yojimbomb

	class PeriodMetric < Yojimbomb::Metric
		attr_reader :stop, :duration
		
		def initialize(type, start, stop, duration, sundry = {})
			super(type, :period, start, sundry)
			@stop = Yojimbomb::DateTime.parseTime(stop)
			
			@duration = if (duration == :whole)
				(self.stop - self.start) / 60
			else
				duration
			end
			
			populateTod(start, stop, sundry)
		end
		
		def populateTod(rawStart, rawStop, sundry = {})
			todStartKey = nil
			[:utcTodStart, :todStart, :tod].each do |startKey|
				next unless sundry.keys.include?(startKey)
				todStartKey = startKey
				break
			end
			
			@todStart = unless todStartKey.nil?
				rawTodStart = sundry[todStartKey]
				raise :invalidTodValue if rawTodStart < 0
				raise :invalidTodValue if rawTodStart > 23.99
				
				unless (todStartKey == :utcTodStart) || (rawStart.utc_offset == 0)
					todStartTime = Yojimbomb::DateTime.changeToTimeOfDay(rawStart, rawTodStart)
					Yojimbomb::DateTime.timeOfDay(todStartTime)
				else
					rawTodStart
				end
			else
				Yojimbomb::DateTime.timeOfDay(self.start)
			end
			
			todStopKey = nil
			[:utcTodStop,:todStop].each do |stopKey|
				next unless sundry.keys.include?(stopKey)
				todStopKey = stopKey
				break
			end
			
			@todStop = unless todStopKey.nil?
				rawTodStop = sundry[todStopKey]
				raise :invalidTodValue if rawTodStop < 0
				raise :invalidTodValue if rawTodStop > 23.99
				
				@todStop = unless (todStopKey == :utcTodStop) || (rawStop.utc_offset == 0)
					todStopTime = Yojimbomb::DateTime.changeToTimeOfDay(rawStop, rawTodStop)
					Yojimbomb::DateTime.timeOfDay(todStopTime)
				else
					rawTodStop
				end
			else
				Yojimbomb::DateTime.timeOfDay(self.stop)
			end
		end
		
		alias :start :occurence
		alias :startDayOfWeek :dayOfWeek
		
		def timeOfDay(zone = nil)
			return @todStart if zone.nil?
			tod_time = Yojimbomb::DateTime.changeToTimeOfDay(self.start,@todStart)
			Yojimbomb::DateTime.timeOfDay(tod_time,zone)
		end
		alias :startTimeOfDay :timeOfDay
				
		def stopTimeOfDay(zone = nil)
			return @todStop if zone.nil?
			tod_time = Yojimbomb::DateTime.changeToTimeOfDay(self.stop,@todStop)
			Yojimbomb::DateTime.timeOfDay(tod_time,zone)
		end
		
		def stopDayOfWeek(zone = nil)
			Yojimbomb::DateTime.dayOfWeek(@stop, zone)
		end
		
		def matchOccurence(criteria)
			super(criteria) && (self.stop <= criteria.stop)
		end
		
		def matchTimeOfDay(criteria) 
			return true if criteria.todStart.nil?
			
			stod = self.startTimeOfDay(criteria.timezone)
			etod = self.stopTimeOfDay(criteria.timezone)
			cstod = criteria.todStart
			cetod = criteria.todStop
			
			bounds = []
			if cetod < cstod
				bounds << [cstod, 23.99 ]
				bounds << [0.0, cetod]
			else
				bounds << [cstod, cetod]
			end

			res = false
			bounds.each do |bstod, betod|
				break if res
				res |= (stod >= bstod) && (stod <= betod)
				res |= (etod >= bstod) && (etod <= betod)				
			end
			res
		end
		
		def matchDayOfWeek(criteria)
			return true if criteria.dow.nil?
			
			my_swd = Yojimbomb::DateTime.numericWeekDay( self.startDayOfWeek(criteria.timezone) )
			my_ewd = Yojimbomb::DateTime.numericWeekDay( self.stopDayOfWeek(criteria.timezone) )
			
			range = if my_swd < my_ewd
				(my_swd..my_ewd).to_a
			else
				sow = Yojimbomb::DateTime.numericWeekDay(Yojimbomb::DateTime::DaysOfWeek[0])
				eow = Yojimbomb::DateTime.numericWeekDay(Yojimbomb::DateTime::DaysOfWeek[-1])
				(my_swd..eow).to_a + (sow..my_ewd).to_a					  
			end
			
			c_wd = Yojimbomb::DateTime.numericWeekDay(criteria.dow)
			range.include?(c_wd)
		end
		
	end
	
	
end
