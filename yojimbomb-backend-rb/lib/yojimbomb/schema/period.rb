
require 'yojimbomb/schema/base'

module Yojimbomb

	class PeriodMetric < Yojimbomb::Metric
		attr_reader :stop, :duration
		
		def initialize(type, start, stop, duration, sundry = {})
			super.initialize(type, :period, start, sundry)
			@duration = duration
			@stop = Yojimbomb::DateTime.parseTime(stop)
		end
		
		alias :start :occurence
		alias :startDayOfWeek :dayOfWeek
		
		def endDayOfWeek
			Yojimbomb::DateTime.dayOfWeek(@stop)
		end
		
		def alter(duration, stop)
			@duration = duration
			@stop = Yojimbomb::DateTime.parseTime(stop)
		end
		
	end
	
	
end
