
require 'securerandom'
require 'yojimbomb/schema/datetime'

module Yojimbomb

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
		
	end
	
	
end

