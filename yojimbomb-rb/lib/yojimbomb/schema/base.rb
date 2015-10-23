
require 'securerandom'
require 'yojimbomb/schema/datetime'

module Yojimbomb

	# a nonsense error is raised/thrown whenver a nonsensical situation is created;
	#	eg. declaring a stop time before a start time.
	class NonsenseError < StandardError
		
	end
	
	class Metric
		attr_reader :metricType, :metricClass, :count, :occurence, :id
		
		DefaultSundry = {:count => 1, :primary => [], :minor => []}
		
		def initialize(mt, mc, occur, sundry = {})
			@metricType = mt.to_sym
			@metricClass = mc.to_sym
			@occurence = Yojimbomb::DateTime.parseTime(occur)
			
			xsundry = DefaultSundry
			xsundry.merge!(sundry) unless sundry.nil?
			@count = xsundry[:count]
			@primaryTags = xsundry[:primary]
			@minorTags = xsundry[:minor]
				
			@id = xsundry[:id].nil? ? SecureRandom.uuid() : xsundry[:id]
		end
		protected :initialize
		
		alias :metric_type :metricType
		alias :type :metricType
		
		alias :metric_class :metricClass
		
		def primaryTags
			@primaryTags.dup
		end
		alias :primary_tags :primaryTags
		alias :tags :primaryTags
		
		
		def minorTags
			@minorTags.dup
		end
		alias :minor_tags :minorTags
		
		def dayOfWeek(zone = nil)
			Yojimbomb::DateTime.dayOfWeek(@occurence, zone)
		end
		
		def timeOfDay(zone = nil)
			Yojimbomb::DateTime.timeOfDay(@occurence,zone)
		end
		
		def match(criteria)
			[
				matchOccurence(criteria),
				matchTimeOfDay(criteria),
				matchDayOfWeek(criteria),
				matchPrimaryTags(criteria),
				matchMinorTags(criteria)
			].reduce(:&)
		end
		
		def matchOccurence(criteria)
			(@occurence >= criteria.start) and (@occurence <= criteria.stop)
		end
		
		def matchTimeOfDay(criteria)
			return true if criteria.todStart.nil?
			tod = self.timeOfDay(criteria.timezone)
			(tod >= criteria.todStart) and (tod <= criteria.todStop)
		end
		
		def matchDayOfWeek(criteria)
			return true if criteria.dow.nil?
			my_wd = Yojimbomb::DateTime.numericWeekDay(self.dayOfWeek(criteria.timezone))
			c_wd = Yojimbomb::DateTime.numericWeekDay(criteria.dow) 
			my_wd == c_wd
		end
		
		def matchPrimaryTags(criteria)
			return true if criteria.primaryTags.empty?
			criteria.primaryTags.each {|cpt| return false unless @primaryTags.include?(cpt) }
			true
		end
		
		def matchMinorTags(criteria)
			return true if criteria.minorTags.empty?
			criteria.minorTags.each {|cmt| return false unless @minorTags.include?(cmt) }
			true
		end
		
	end
	
	class Criteria
		attr_reader :start, :stop
		attr_reader :todStart, :todStop, :dow, :timezone
		attr_reader :primaryTags, :minorTags
		
		TodDefaultIncrement = 30
		DefaultSundry = {
			:todIncrement => 1,
			:timezone => 0
		}
		
		def initialize(start,stop, sundry = {})
			@start = Yojimbomb::DateTime.parseTime(start)
			@stop = Yojimbomb::DateTime.parseTime(stop)
			
			xsundry = DefaultSundry.dup
			unless sundry.nil?
				csundry = sundry.select {|k,v| !v.nil?}
				xsundry.merge!(csundry)
			end
			
			@todStart = xsundry[:todStart]
			@todStart = xsundry[:tod] if todStart.nil?
			
			unless todStart.nil?
				@todStop = xsundry[:todStop]
				@todStop = todStart + xsundry[:todIncrement] if @todStop.nil?
			end
				
			@dow = xsundry[:dow]
				
			@timezone = xsundry[:timezone] 
				
			@primaryTags = []
			@primaryTags += xsundry[:primaryTags] unless xsundry[:primaryTags].nil?
				
			@minorTags = []
			@minorTags += xsundry[:minorTags] unless xsundry[:minorTags].nil?
		end
	
	end
	
end

