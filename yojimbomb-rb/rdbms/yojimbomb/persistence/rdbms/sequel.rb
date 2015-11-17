
require 'yojimbomb'

require 'sequel'

module Yojimbomb
module RDBMS
	
	#
	# Sequel based RDBMS integration
	# 
	# Implemenation notes:
	# 	* uses a dynamic table creation system
	# 	* per implemenation, minor tags are effectivly indexed
	#
	class SequelMetricsKeeper < Yojimbomb::MetricsKeeper
		
		#TODO: eventually use this to swap out column types & other per-dialect adaptations
		SupportedDialects = [
			:mysql, :mysql2,
		]
		
		attr_accessor :tablePrefix
		
		def self.connect(*connect, &cblock)
			self.new( Sequel.connect(*connect, &cblock) )
		end
		
		def initialize(db, dialect = nil)
			super()
			Sequel.default_timezone = :utc
			@db = db
			@dialect = dialect.nil? ? @db.adapter_scheme : dialect
			raise "invalidDbDialect: #{@dialect}" unless SupportedDialects.include?(@dialect)
			
			@dataTypes = {
				:uint => 'integer(10) unsigned',
				:guid => 'varbinary(16)',
				:symbol => 'char(10)',
				:searchLabel => 'char(32)',
				:timestamp => 'timestamp',
				:todValue => 'integer(4) unsigned'
			}
		end
		
		def serializeId(id)
			hex = id.to_s(16)
			hex.insert(0, '0' * (32 - hex.size))
			hex.scan(/../).map {|hxc| hxc.hex.chr}.join.force_encoding('utf-8') 
		end
		def parseId(rawid)
			str = rawid.each_byte.map do |b|
				hx = b.to_s(16)
				hx.insert(0,'0' * (2 - hx.size))	
			end.join
			Yojimbomb.idValue(str)
		end
		
		def cleanDbSym(id) 
			id.to_s.downcase.strip[0..9]
		end
		
		def tableName(*nameParts)
			prefix = self.cleanDbSym( @tablePrefix.nil? ? '' : "#{@tablePrefix}_")
			cnparts = nameParts.map {|p| cleanDbSym(p) }
			"#{prefix}#{cnparts.join('_')}".to_sym
		end
		
		[:period,:event].each do |mc| defineEnsureMetricClass(mc) do |metricClass|
			dataTypes = @dataTypes
			@db.create_table? tableName('meta', metricClass) do
				primary_key(:pk, dataTypes[:uint])
				column(:metricType, dataTypes[:symbol], :unique => true)
			end
			self
		end end
		
		def createSearchTables(mtype,mclass)
			dataTypes = @dataTypes
			
			mainSearchTable = tableName(mclass,mtype,'sch')
			ptagSearchTable = tableName(mclass,mtype,'pti')
			mtagSearchTable = tableName(mclass,mtype,'mti')
			idSearchTable = tableName(mclass,mtype,'idi')
			
			@db.create_table? mainSearchTable do 
				primary_key(:pk, dataTypes[:uint])
				column(:label, dataTypes[:searchLabel])
				column(:created, dataTypes[:timestamp])
			end
			
			[ptagSearchTable, mtagSearchTable].each do |tagSearchTable| @db.create_table? tagSearchTable do
				primary_key(:pk, :type => dataTypes[:uint])
				foreign_key(:sid, mainSearchTable, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
				
				column(:tagv, dataTypes[:symbol], :null => false)
				unique([:sid, :tagv])
			end end
			
			@db.create_table? idSearchTable do
				primary_key(:pk, dataTypes[:uint])
				foreign_key(:sid, mainSearchTable, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
				
				column(:mid, dataTypes[:guid], :null => false)
				unique([:sid, :mid])
			end
			
		end
		
		def createTagTables(mtype, mclass)
			dataTypes = @dataTypes
			
			mnTable = tableName(mclass, mtype)
			
			#minor tag main, link, & search tables
			mtags  = tableName(mclass,mtype,'mt')
			mtagsx = tableName(mclass,mtype,'mtx')
			
			#primary tag main, link, & search tables
			ptags  = tableName(mclass,mtype,'pt')
			ptagsx = tableName(mclass,mtype,'ptx')
			
			[ [ptags,ptagsx], [mtags,mtagsx] ].each do |tags,tagx|
				@db.create_table? tags do 
					primary_key(:pk, dataTypes[:uint])
					column(:tagv, dataTypes[:symbol], :unique => true)
				end
				@db.create_table? tagx do
					foreign_key(:tid, tags, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
					foreign_key(:mid, mnTable, :type => dataTypes[:guid], :null => false, :on_delete => :cascade)
					primary_key([:tid,:mid])
				end
			end
		end
		
		defineEnsureMetricType(:event) do |metricType, mclass|
			dataTypes = @dataTypes
			
			meta = tableName('meta', mclass)
			table  = tableName(mclass,metricType)
			rmTracker = tableName(mclass,metricType, 'rm')
			
			if @db[meta][:metricType => cleanDbSym(metricType)].nil?
				@db[meta] << {:metricType => cleanDbSym(metricType)}
					
				@db.create_table?(table) do
					column(:id, dataTypes[:guid], :null => false)
					primary_key([:id])
					
					column(:count, dataTypes[:uint], :null => false)
					column(:occur, dataTypes[:timestamp])
					column(:qty, dataTypes[:uint], :null => false)
				end
				
				createTagTables(metricType, mclass)
				createSearchTables(metricType, mclass)
			end
			
			self
		end
		
		defineEnsureMetricType(:period) do |metricType, metricClass|
			dataTypes = @dataTypes
			
			meta = tableName('meta', metricClass)
			table  = tableName(metricClass, metricType)
			rmTracker = tableName(metricClass, metricType, 'rm')
			
			if @db[meta][:metricType => cleanDbSym(metricType)].nil?
				@db[meta] << {:metricType => cleanDbSym(metricType)}
					
				@db.create_table?(table) do
					column(:id, dataTypes[:guid], :null => false)
					primary_key([:id])
					
					column(:count, dataTypes[:uint], :null => false)
					[:pstart,:pstop].each {|col| column(col, dataTypes[:timestamp]) }
					[:todstart,:todstop].each {|col| column(col, dataTypes[:todValue])}
					column(:dur, dataTypes[:uint])
				end
				
				createTagTables(metricType, metricClass)
				createSearchTables(metricType, metricClass)
			end
			
			self
		end
		
		defineStore(:event) do |mtype, *events|
			mclass = :event
			table = tableName(mclass, mtype)
			events.each do |event|
			self.tryBlock("unable to store metric(#{mtype}/#{mclass}) #{event}") do
				ids = serializeId(event.id)
				@db[table] << {
					:id => serializeId(event.id),
					:count => event.count, 
					:occur => event.occurence,
					:qty => event.quantity
				}
				self.persistTags(event)
			end end
			
			self
		end
		
		defineStore(:period) do |mtype, *periods|
			mclass = :period
			table = tableName(mclass, mtype)
			
			periods.each do |period|
			self.tryBlock("unable to store metric(#{mtype}/#{mclass}) #{period}") do
				dbStod = (period.startTimeOfDay * 100).to_i
				dbEtod = (period.stopTimeOfDay * 100).to_i
				@db[table] << {
					:id => serializeId(period.id),
					:count => period.count, 
					:pstart => period.start, :pstop => period.stop,
					:todstart => dbStod, :todstop => dbEtod,
					:dur => period.duration,
				}
				self.persistTags(period)
			end end
			
			self
		end
		
		def persistTags(metric)
			mtype, mclass = metric.metricType, metric.metricClass
			ids = serializeId(metric.id)
			
			metricTb = tableName(mclass, mtype)
			mtb  = tableName(mclass, mtype, 'mt')
			mxtb = tableName(mclass, mtype, 'mtx')
			ptb  = tableName(mclass, mtype, 'pt')
			pxtb = tableName(mclass, mtype, 'ptx')
			
			[
				[mtb, mxtb, metric.minorTags],
				[ptb, pxtb, metric.primaryTags]
			].each do |tb, xtb, tags| tags.uniq.each do |tag|
				tagv = cleanDbSym(tag)
				
				tagid = nil
				tagRow = @db[tb][:tagv => tagv]
				tagid = tagRow[:pk] unless tagRow.nil?
				if tagid.nil?
					@db[tb] << {:tagv => tagv}
					tagid = @db[tb][:tagv => tagv][:pk]
				end
				
				if @db[xtb][:tid => tagid, :mid => serializeId(metric.id) ].nil?
					@db[xtb] << {:tid => tagid, :mid => serializeId(metric.id)}	
				end
			end end
			
			self
		end
		
		def openSearch(metricType, metricClass)
			searchMainTable = tableName(metricClass, metricType, 'sch')
			slabel = Yojimbomb.idValue.to_s(16)
			@db[searchMainTable] << {:label => slabel, :created => Time.now}
			@db[searchMainTable][:label => slabel][:pk]
		end
		
		def closeSearch(metricType, metricClass, searchId)
			searchMainTable = tableName(metricClass, metricType, 'sch')
			@db[searchMainTable].where(:pk => searchId).delete
		end
		
		[:event,:period].each do |mclass| defineGetMetricTypes(mclass) do
			meta = tableName('meta',mclass)
			@db[meta].map(:metricType).map {|mt| mt.to_sym}
		end end
		
		def buildTagSearch(metricType, metricClass, tagsym, searchId, *tags)
			table = tableName(metricClass, metricType,"#{tagsym}ti")
			
			raws = tags.map do |tagv|
				{:sid => searchId, :tagv => tagv}
			end
			@db[table].multi_insert(raws)
			
			table
		end
		
		def buildIdSearch(metricType, metricClass, searchId, *ids)
			table = tableName(metricClass, metricType, 'idi')
			
			raws = ids.map do |id|
				{:sid => searchId, :mid => serializeId(id) }
			end
			@db[table].multi_insert(raws)
			
			table
		end
		
		def getTagData(metricType, metricClass, searchId, *metricIds)
			results = Hash.new do |h,k|
				h[k] = {:primary => [], :minor => []}
			end
			
			idSearchTable = buildIdSearch(metricType, metricClass, searchId, *metricIds)
			
			{
				'p' => :primary,
				'm' => :minor
			}.each do |sym,tagcat|
				mnTbl = tableName(metricClass, metricType, "#{sym}t")
				lnTbl = tableName(metricClass, metricType, "#{sym}tx")
				
				raws = @db[
					"select mid, tagv from #{lnTbl} as tx join #{mnTbl} as tm on (tx.tid = tm.pk)"
				].where("tx.mid in (select mid from #{idSearchTable} where sid = ?)", searchId)
			
				raws.each {|raw| results[ parseId(raw[:mid]) ][tagcat] << raw[:tagv].to_sym }
			end
			
			results
		end
		
		def findRaw(metricType, metricClass, criteria, startTimeField, stopTimeField)
			table = tableName(metricClass, metricType)
			searchId = openSearch(metricType, metricClass)
			dset = @db[table]
			
			#initial, dataset based filtering
			#	(in other words, in the db itself)
			#	note that this does NOT include dow & tod based filtering,
			#	which is done post-query to avoid extra dicking around w/ timezones in db
			unless criteria.nil?
				dset = dset.where {startTimeField >= criteria.start}
				dset = dset.where {stopTimeField <= criteria.stop}
				
				{
					'p' => criteria.primaryTags,
					'm' => criteria.minorTags
				}.each do |tagsym, tags|
					next if tags.empty?
					searchTbl = buildPrimaryTagSearch(metricType, metricClass, tagsym, searchId, *tags)
					tagMnTbl = tableName(metricClass, metricType, "#{tagsym}t")
					tagLnTbl = tableName(metricClass, metricType, "#{tagsym}tx")
					dset = dset.where(
						"id in (select tx.mid from #{tagMnTbl} as tm join #{tagLnTbl} as tx on (tx.tid = tm.pk) where tm.tagv in (select tagv from #{searchTbl} where sid = ?))",
						searchId
					)
				end
			end
			
			results = []
			indexResults = {}
			
			dset.each do |raw|
				id = parseId(raw[:id])
				raw[:id] = id
				results << raw
				indexResults[id] = raw
			end
			
			tagResults = getTagData(metricType, metricClass, searchId, *indexResults.keys)
			tagResults.each {|id, tagData| indexResults[id].merge!(tagData) }
			
			closeSearch(metricType, metricClass, searchId)
			
			results
		end
		
		defineFind(:event) do |metricType, criteria|
			results = []
			raws = findRaw(metricType, :event, criteria, :occur, :occur)
			
			raws.each do |raw|
				occur = raw[:occur]
				sundry = raw
				sundry[:quantity] = raw[:qty]
				results << Yojimbomb::EventMetric.new(metricType, occur, sundry)
			end
			
			results
		end
		
		defineFind(:period) do |metricType, criteria|
			results = []
			raws = findRaw(metricType, :period, criteria, :pstart, :pstop)
			
			raws.each do |raw|
				start = raw[:pstart]
				stop = raw[:pstop]
				duration = raw[:dur]
				
				xStod = (raw[:todstart] / 100).to_i
				
				sundry = raw
				sundry[:todStart] = raw[:todstart].to_f / 100
				sundry[:todStop]  = raw[:todstop].to_f / 100
				raw.delete(:todstart)
				raw.delete(:todstop)
				sundry.merge!(raw)
				
				results << Yojimbomb::PeriodMetric.new(metricType, start, stop, duration, sundry)
			end
			
			results
		end
		
		[:event, :period].each do |metricClass| defineRemove(metricClass) do |metricType, mclass, *ids|
			mnTable = tableName(mclass,metricType)
			searchId = openSearch(metricType, mclass) 
			idSearchTable = buildIdSearch(metricType, metricClass, searchId, *ids)
			@db[mnTable].where("id in (select mid from #{idSearchTable} where sid=?)", searchId).delete 
			closeSearch(metricType, mclass, searchId)
			self
		end end
	
	end
	
end end 

