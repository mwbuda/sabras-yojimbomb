
require 'yojimbomb/schema/base'

module Yojimbomb

	class PeriodMetric < Yojimbomb::Metric
		attr_reader :stop, :duration
		
		def initialize(type, start, stop, duration, sundry = {})
			super.initialize(type, :period, start, sundry)
			@stop = Yojimbomb::DateTime.parseTime(stop)
			
			if (duration == :whole)
				@duration = (self.stop - self.start)
				@todStart = Yojimbomb::DateTime.timeOfDay(self.start)
				@todStop = Yojimbomb::DateTime.timeOfDay(self.stop)
			else
				@duration = duration
			
				if sundry.keys.include?(:tod) or sundry.keys.include?(:todStart)
					@todStart = sundry[:todStart]
					@todStart = sundry[:tod] if @todStart.nil?
					@todStop = sundry[:todStop]	
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
			Yojimbomb::DateTime.timeOfDay(tod_time,criteria.zone)
		end
		alias :startTimeOfDay :timeOfDay
				
		def stopTimeOfDay(zone = nil)
			tod_time = Yojimbomb::DateTime.changeToTimeOfDay(self.start,@todStop)
			Yojimbomb::DateTime.timeOfDay(tod_time,criteria.zone)
		end
		
		def stopDayOfWeek(zone = nil)
			Yojimbomb::DateTime.dayOfWeek(@stop, zone)
		end
		
		def matchOccurence(criteria)
			super(criteria) and (self.stop <= criteria.stop)
		end
		
		def matchTimeOfDay(criteria) 
			return true if critera.todStart.nil?
			
			stod = self.startTimeOfDay(criteria.zone)
			etod = self.stopTimeOfDay(critera.zone)
			
			if (stod >= criteria.todStart) and (stod <= criteria.todStop)
				true
			elsif (etod >= criteria.todStart) and (etod <= criteria.todStop)
				true
			else
				false
			end
		end
		
		def matchDayOfWeek(criteria)
			return true if criteria.dow.nil?
			
			my_swd = Yojimbomb::DateTime.numericWeekDay( self.startDayOfWeek(criteria.zone) )
			my_ewd = Yojimbomb::DateTime.numericWeekDay( self.endDayOfWeek(criteria.zone) )
			
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
