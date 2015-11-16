
require 'yojimbomb/schema/base'

module Yojimbomb

	class PeriodMetric < Yojimbomb::Metric
		attr_reader :stop, :duration
		
		def initialize(type, start, stop, duration, sundry = {})
			super(type, :period, start, sundry)
			@stop = Yojimbomb::DateTime.parseTime(stop)
			
			if (duration == :whole)
				@duration = (self.stop - self.start)
				@todStart = Yojimbomb::DateTime.timeOfDay(self.start)
				@todStop = Yojimbomb::DateTime.timeOfDay(self.stop)
			else
				@duration = duration
			
				if sundry.keys.include?(:tod) or sundry.keys.include?(:todStart)
					rawTodStart = sundry[:todStart]
					rawTodStart = sundry[:tod] if rawTodStart.nil?
					
					todStartTime = Yojimbomb::DateTime.changeToTimeOfDay(start, rawTodStart)
					@todStart = Yojimbomb::DateTime.timeOfDay(todStartTime)
					
					rawTodStop = sundry[:todStop]
					unless rawTodStop.nil?
						todStopTime = Yojimbomb::DateTime.changeToTimeOfDay(start, rawTodStop)
						@todStop = Yojimbomb::DateTime.timeOfDay(todStopTime)
					end	
				else
					@todStart = Yojimbomb::DateTime.timeOfDay(self.start)
				end
				
				if @todStop.nil?
					num_days = Yojimbomb::DateTime.daysBetween(self.start,self.stop) + 1
					mins_per_day = @duration / num_days
					elapsed_change = ((mins_per_day * 10) / 6).to_f / 100.0
					@todStop = @todStart + elapsed_change
				end
			end
		end
		
		alias :start :occurence
		alias :startDayOfWeek :dayOfWeek
		
		def timeOfDay(zone = nil)
			tod_time = Yojimbomb::DateTime.changeToTimeOfDay(self.start,@todStart)
			Yojimbomb::DateTime.timeOfDay(tod_time,zone)
		end
		alias :startTimeOfDay :timeOfDay
				
		def stopTimeOfDay(zone = nil)
			tod_time = Yojimbomb::DateTime.changeToTimeOfDay(self.start,@todStop)
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
