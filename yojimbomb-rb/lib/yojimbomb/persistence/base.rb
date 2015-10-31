

module Yojimbomb
	
	
	class MetricsKeeper
	
		def store(*metrics)
			errors = []
			
			eventsToStore = []
			periodsToStore = []
			errors = false
				
			metrics.each do |metric| 
				group = case metric
					when Yojimbomb::EventMetric then eventsToStore
					when Yojimbomb::PeriodMetric then periodsToStore
					else false		
				end 
				
				unless group
					errors = true
					next
				end
				
				group << metric
			end
			
			self.storeEvents(*eventsToStore)
			self.storePeriods(*periodsToStore)
			
			raise :invalidMetrics if errors
		end
		
		def storeEvents(*events)
			raise :unimplemented
		end
			
		def findEvents(criteria)
			criteria.filter(*self.getAllEvents)
		end
		
		def getAllEvents()
			raise :unimplemented
		end
		
		def storePeriods(*periods)
			raise :unimplemented
		end
		
		def findPeriods(criteria)
			criteria.filter(*self.getAllPeriods)
		end
		
		def getAllPeriods()
			raise :unimplemented
		end
		
	end
	
end

