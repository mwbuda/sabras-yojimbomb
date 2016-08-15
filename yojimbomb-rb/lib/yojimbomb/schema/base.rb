
require 'securerandom'
require 'yojimbomb/schema/datetime'

module Yojimbomb

	MaxId = ('ff'*16).to_i(16)
	MinId = 0
	InvalidTagChars = /(?:[^a-z0-9]|[^_=.$%&\#@+*]|[-])+/i

	def self.idValue(input = SecureRandom.uuid.gsub(/[-]/, ''))
		cand = case input
			when Symbol then input.to_s.to_i(16)	
			when String then input.to_i(16)
			when nil then Yojimbomb.idValue()
			when Numeric then input.to_i
			else nil
		end
		
		raise :invalidId if cand.nil?
		raise :invalidId if cand > Yojimbomb::MaxId
		raise :invalidId if cand < Yojimbomb::MinId 
		cand
	end
	
	def self.tagValue(tag)
		xtag = tag
		{
			/\s+/ => '_',
			/[_]{2,}/ => '_',
			Yojimbomb::InvalidTagChars => ''
		}.each {|clean,repl| xtag = xtag.gsub(clean, repl) }
		xtag = xtag.to_s.downcase.strip[0..15]
		xtag.empty? ? nil : xtag
	end
	
	def self.tagValues(*tags)
		tags.map {|tag| Yojimbomb::tagValue(tag)}.compact.uniq
	end


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
			@primaryTags = Yojimbomb::tagValues( *xsundry[:primary] )
			@minorTags = Yojimbomb::tagValues( *xsundry[:minor] )
				
			@id = Yojimbomb.idValue(xsundry[:id])
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
		def withPrimaryTags(*tags)
			@primaryTags += Yojimbomb::tagValues(*tags)
			@primaryTags.uniq!
			self
		end
		alias :withTags :withPrimaryTags
		
		def minorTags
			@minorTags.dup
		end
		alias :minor_tags :minorTags
		def withMinorTags(*tags)
			@minorTags += Yojimbomb::tagValues(*tags)
			@minorTags.uniq!
			self
		end
		
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
			(@occurence >= criteria.start) && (@occurence <= criteria.stop)
		end
		
		def matchTimeOfDay(criteria)
			return true if criteria.todStart.nil?
			
			tod = self.timeOfDay(criteria.timezone)
			cstod = criteria.todStart
			cetod = criteria.todStop
			
			bounds = []
			if (cetod < cstod)
				bounds << [cstod, 23.99]
				bounds << [0.0, cetod]
			else
				bounds << [cstod, cetod]
			end
			
			res = false
			bounds.each do |bstod, betod|
				break if res
				res |= (tod >= bstod) && (tod <= betod)
			end
			res
		end
		
		def matchDayOfWeek(criteria)
			return true if criteria.dow.nil?
			my_wd = Yojimbomb::DateTime.numericWeekDay(self.dayOfWeek(criteria.timezone))
			c_wd = Yojimbomb::DateTime.numericWeekDay(criteria.dow) 
			my_wd == c_wd
		end
		
		def matchPrimaryTags(criteria)
			return true if criteria.primaryTags.empty?
			contains = criteria.primaryTags.uniq.map {|cmt| @primaryTags.include?(cmt)}
			contains.reduce(:&)
		end
		
		def matchMinorTags(criteria)
			return true if criteria.minorTags.empty?
			contains = criteria.minorTags.uniq.map {|cmt| @minorTags.include?(cmt)}
			contains.reduce(:&)
		end
		
	end
	
	class Criteria
		attr_reader :start, :stop
		attr_reader :todStart, :todStop, :dow, :timezone
		
		TodDefaultIncrement = 30
		DefaultSundry = {
			:todIncrement => 1,
			:timezone => nil
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
				@todStart = @todStart.to_i + ( ((@todStart * 100).to_i % 100).to_f / 100.0)
				@todStop  = @todStop.to_i  + ( ((@todStop  * 100).to_i % 100).to_f / 100.0)
				raise :invalidTodValue if @todStart < 0
				raise :invalidTodValue if @todStop < 0
				raise :invalidTodValue if @todStart > 23.99
				raise :invalidTodValue if @todStop > 23.99 
			end
				
			@dow = xsundry[:dow]
				
			@timezone = xsundry[:timezone] 
				
			@primaryTags = []
			@primaryTags += Yojimbomb::tagValues(*xsundry[:primary]) unless xsundry[:primary].nil?
				
			@minorTags = []
			@minorTags += Yojimbomb::tagValues(*xsundry[:minor]) unless xsundry[:minor].nil?
		end
	
		def filter(*metrics)
			metrics.select {|metric| metric.match(self)}
		end
		
		def primaryTags()
			@primaryTags.dup
		end
		
		def minorTags()
			@minorTags.dup
		end
		
	end
	
end

