
require 'yojimbomb/schema/base'

module Yojimbomb

	class EventMetric < Yojimbomb::Metric
		attr_reader :quantity
		
		def initialize(type, occur, sundry = {})
			super(type, :event, occur, sundry)
			@quantity = sundry[:quantity].nil? ? 0 : sundry[:quantity]
		end
		
	end
	
end
