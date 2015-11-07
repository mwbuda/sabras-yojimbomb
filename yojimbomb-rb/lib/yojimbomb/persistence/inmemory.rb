
require 'yojimbomb/persistence/base'

module Yojimbomb
	

	class InMemMetricsKeeper < Yojimbomb::MetricsKeeper
		
		module Logic
			EnsureMetricClass = Proc.new do |metricClass|
				@index[metricClass] = []
				@content[metricClass] = {}
				self
			end
			
			EnsureMetricType = Proc.new do |metricType, metricClass|
				@index[metricClass] << metricType
				@content[metricClass][metricType] = []
				self
			end
			
			Store = Proc.new do |metricType, metricClass, *metrics|
				store = @content[metricClass][metricType]
				alreadyHave = store.map {|metric| metric.id}
				toAdd = metrics.reject {|metric| alreadyHave.include?(metric.id)}
				store.insert(-1, toAdd)
				self
			end
			
			GetMetricTypes = Proc.new do |metricClass|
				@index[metricClass]
			end
			
			Find = Proc.new do |metricType, metricClass, criteria|
				@content[metricClass][metricType].dup
			end
			
			Remove = Proc.new do |metricType, metricClass, *mids|
				@content[metricClass][metricType].reject! {|metric| mids.include?(metric.id) }
				self
			end
			
		end
		
		def initialize()
			@content = {}
			@index = {}
		end
		
		def self.supportMetricClass(metricClass)
			self.defineEnsureMetricClass(metricClass, &Logic::EnsureMetricClass)
			self.defineEnsureMetricType(metricClass, &Logic::EnsureMetricType)
			self.defineStore(metricClass, &Logic::Store)
			self.defineGetMetricTypes(metricClass, &Logic::GetMetricTypes)
			self.defineFind(metricClass, &Logic::Find)
			self.defineRemove(metricClass, &Logic::Remove)
			self
		end
	
		def supportMetricClass(metricClass)
			self.defineEnsureMetricClass(metricClass, &Logic::EnsureMetricClass)
			self.defineEnsureMetricType(metricClass, &Logic::EnsureMetricType)
			self.defineStore(metricClass, &Logic::Store)
			self.defineGetMetricTypes(metricClass, &Logic::GetMetricTypes)
			self.defineFind(metricClass, &Logic::Find)
			self.defineRemove(metricClass, &Logic::Remove)
			self
		end
		
		supportMetricClass(:event)
		supportMetricClass(:period)
		
	end	
	
end
