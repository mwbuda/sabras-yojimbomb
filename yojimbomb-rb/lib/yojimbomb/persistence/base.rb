
require 'yojimbomb/schema/all'
require 'logger'

module Yojimbomb
	
	class MetricsKeeper
		DefaultLoggerOut = StringIO.new
		DefaultLogger = Logger.new(Yojimbomb::MetricsKeeper::DefaultLoggerOut)
		
		attr_writer :logger
		
		def logger()
			@logger.nil? ? @nilLogger : @logger
		end
		
		def self.defineEnsureMetricClass(&block)
			@ensureMetricClass = block
			self
		end
		def self.ensureMetricClassLogic()
			@ensureMetricClass
		end
		def defineEnsureMetricClass(&block)
			@ensureMetricClass
			self
		end
		
		def self.defineEnsureMetricType(metricClass, &block)
			@ensureMetricType = {} if @ensureMetricType.nil?
			@ensureMetricType[metricClass] = block
			self		
		end
		def self.ensureMetricTypeLogic(metricClass)
			@ensureMetricType = {} if @ensureMetricType.nil?
			@ensureMetricType[metricClass]
		end
		def defineEnsureMetricType(metricClass, &block)
			@ensureMetricType[metricClass] = block
			self
		end
		
		def self.defineStore(metricClass, &block)
			@store = {} if @store.nil?
			@store[metricClass] = block
			self
		end
		def self.storeLogic(metricClass)
			@store = {} if @store.nil?
			@store[metricClass]
		end
		def defineStore(metricClass, &block)
			@store[metricClass] = block
			self
		end
		
		def self.defineGetMetricTypes(metricClass, &block)
			@getMetricTypes = {} if @getMetricTypes.nil?
			@getMetricTypes[metricClass] = block
			self
		end
		def self.getMetricTypesLogic(metricClass)
			@getMetricTypes[metricClass]
		end
		def defineGetMetricTypes(metricClass, &block)
			@getTypes[metricClass] = block
			self
		end
		
		def self.defineFindAll(metricClass, &block)
			@findAll = {} if @findAll.nil?
			@findAll[metricClass] = block
			self
		end
		def self.findAllLogic(metricClass)
			@findAll[metricClass]
		end
		def defineFindAll(metricClass, &block)
			@findAll[metricClass] = block
			self
		end
		
		def self.defineFind(metricClass, &block)
			@find = Hash.new do |h,mclass|
				Proc.new {|mtype, criteria| criteria.filter(*self.findAll(mtype, metricClass))} 
			end if @find.nil?
			@find[metricClass] = block
			self
		end
		def self.findLogic(metricClass)
			@find = Hash.new do |h,mclass|
				Proc.new {|mtype, criteria| criteria.filter(*self.findAll(mtype, metricClass))} 
			end if @find.nil?
			@find[metricClass]			
		end
		def defineFind(metricClass, &block)
			@find[metricClass] = block
			self
		end
		
		def self.defineRemove(metricClass, &block)
			@remove = {} if @remove.nil?
			@remove[metricClass] = block
			self
		end
		def self.removeLogic(metricClass)
			@remove = {} if @remove.nil?
			@remove[metricClass]
		end
		def defineRemove(metricClass, &block)
			@remove[metricClass] = block
			self
		end
		
		def initialze()
			@nilLogger = Logger.new(File::NULL)
			@logger = Yojimbomb::MetricsKeeper::DefaultLogger
			
			@ensuredMetricClasses = Hash.new {|h,mclass| h[mclass] = false}
			@ensuredMetricTypes = Hash.new do |h1,mclass|
				h1[mclass] = Hash.new {|h2,mtype| h2[mtype] = false}
			end
				
			@ensureMetricType = {}
			@store = {}
			@getTypes = {}
			@findAll = {}
			@find = Hash.new do |h,mclass|
				h[mclass] = Proc.new {|mtype,criteria| criteria.filter(*self.findAll(mclass))}
			end
			@remove = {}
			
			self.defineEnsureMetricClass(&self.class.ensureMetricClassLogic)
			
			[:event, :period].each do |metricClass|
				self.defineEnsureMetricType(metricClass, &self.class.ensureMetricTypeLogic(metricClass))
				self.defineStore(metricClass, &self.class.storeLogic(metricClass))
				self.defineGetTypes(metricClass, &self.class.getTypesLogic(metricClass))
				self.defineFindAll(metricClass, &self.class.findAllLogic(metricClass))
				self.defineFind(metricClass, &self.class.findLogic(metricClass))
				self.defineRemove(metricClass, &self.class.removeLogic(metricClass))
			end
		end
		
		def ensureMetricClass(metricClass)
			return true if @ensuredMetricClasses[metricClass]
			
			ensured = false
			begin
				ensured = self.instance_exec(metricClass, &@ensureMetricClass)
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to ensure persistence for class=#{metricClass}")
			end
			
			ensured = ensured ? true : false
			@ensuredMetricClasses[metricClass] = ensured
		end
		
		def ensureMetricType(metricType, metricClass)
			emc = self.ensureMetricClass(metricClass)
			return false unless emc
			return true if @ensuredMetricTypes[metricClass][metricType]
			
			ensured = false
			begin
				ensured = self.instance_exec(metricType, &@ensureMetricType[metricClass])
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to ensure persistence for #{metricType}/#{metricClass}")
			end
			
			ensured = ensured ? true : false
			@ensuredMetricTypes[metricClass][metricType] = ensured
		end
		
		#store given metrics into persistence
		#	this method accepts all kinds of metrics, and will sort to the appropriate delegate method
		#		(eg. storeEvents, storePeriods)
		#
		#	Note that a silent failure approach is taken, metricsKeeper will attempt to store all other metrics,
		#		and failure of a given metric to be persisted will be recorded in the logs.
		#		No ERROR will be thrown, and store() should complete successfully
		#		this behavior applies only to metrics whose persistence can not be ensured (ensurePersistence),
		#		and metrics whose metricClass does not evaluate to a valid, handled value
		#		other errors are assumed to be handeld in the same manner, however this is dependent on the implementation
		#		of storeEvents, storePeriods, etc.
		#		
		def store(*metrics)
			groups = Hash.new do |h1,mclass|
				h1[mclass] = Hash.new do |h2,mtype|
					h2[mtype] = Array.new
				end
			end
				
			metrics.each do |metric| 
				mtype = metric.metricType
				mclass = metric.metricClass
				ensured = self.ensureMetricType(mtype,mclass)
				
				unless ensured
					self.logger.error("unable to store metric(#{mtype}/#{mclass}) #{metric}")
					next
				end
				
				groups[mclass][mtype] << metric
			end
			
			groups.each do |mclass,mtypes| mtypes.each do |mtype,metrics|
				storex = self.instance_exec(mtype, *metrics, &@store[mclass])
			end end

			self
		end
		
		def metricTypes(metricClass)
			ensured = self.ensureMetricClass(metricClass)
			unless ensured
				self.logger.error("unable to find metric-types for metric-class=#{mclass}")
				return []
			end
			 
			begin
				return self.instance_exec(&@getTypes[metricClass])
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to find metric-types for metric-class=#{mclass}")
			end
			
			[]
		end
		
		def findAll(metricType, metricClass)
			ensured = self.ensureMetricType(metricType, metricClass) 
			unless ensured
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
				return []
			end
			
			begin
				return self.instance_exec(metricType, &@findAll[metricClass])
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
			end
			
			[]
		end
		
		def find(metricType, metricClass, criteria)
			ensured = self.ensureMetricType(metricType, metricClass) 
			unless ensured
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
				return []
			end
			
			begin
				return self.instance_exec(metricType, criteria, &@find[metricClass])
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
			end
			
			[]
		end
		
		def replace(combMetric, *replIds)
			self.remove(combMetric.metricType, combMetric.metricClass, *replIds)
			self.store(combMetric)
			self
		end
		
		def remove(metricType, metricClass, *metricIds)
			ensured = self.ensureMetricType(metricType,metricClass)
			unless ensured
				self.logger.error("unable to remove metrics of #{mtype}/#{mclass}")
				return self
			end
			
			begin
				self.instance_exec(metricType, *metricIds, &@remove[metricClass])
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to remove metrics of #{mtype}/#{mclass}")
			end
			
			self
		end
		
	end
	
end

