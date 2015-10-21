
require 'rubygems'
require 'minitest'
require 'minitest/autorun'

require '../lib/yojimbomb/schema/datetime'

DtExt = Yojimbomb::DateTime

class DateTimeParseTest
		
	def test_parseRawDate
		date = Date.now
		assertParse(date, date.to_time, "date object")
	end
	
	def test_parseRawTime
		time = Time.now
		assertParse(time, time, "time object")
	end
	
	def test_parseSeconds
		seconds = Random.rand
		assertParse(seconds, Time.at(seconds), 'int seconds' )
		
		fseconds = seconds.to_f + 0.88
		assertParse(fseconds, Time.at(seconds), 'float seconds')
	end
	
	def test_parseRfc2822
		time = Time.now
		assertParse(time.rfc2822, time, 'rfc2822')
	end
	
	def test_parseIso8601
		time = Time.now
		assertParse(time.iso8601, time, 'iso8601')
	end
	
	def assertParse(raw, expResultTime, message)
		actResultTime = DtExt.parseTime(raw)
		refute_nil actResultTime, message
		assert actResultTime.utc?, message
		assert_equals expResultTime.getutc.to_i, actResultTime.getutc.to_i, message
	end
	
	def test_extractDow
		checks = {
			:mon => '3 aug 2015 00:00 EST',
			:tue => '4 aug 2015 00:00 EST',
			:wed => '5 aug 2015 00:00 EST',
			:thr => '6 aug 2015 00:00 EST',
			:fri => '7 aug 2015 00:00 EST',
			:sat => '8 aug 2015 00:00 EST',
			:sun => '9 aug 2015 00:00 EST',
		}
		
		checks.each {|exp, time| assert_equals exp, DtExt.dayOfWeek(DtExt.parseTime(time), '-05:00'), "extract day of week: #{exp}" }
	end
	
	def test_extractHod
		time = Time.now
		assert_equals time.getutc.hour, DtExt.hourOfDay(time), "extract hour of day"
	end
	
	
end

class DateTimeCompressTest
	
	def assertCompress(inTime, expTime, compressType)
		outTime = compressType.apply(inTime)
		refute_nil outTime, "nil result: #{compressType}"
		assert outTime.utc?, "not UTC: #{compressType}"
		assert_equals expTime.to_i, outTime.to_i, "bad result: #{expTime} <=> #{outTime} = #{expTime.to_i-outTime.to_i}: #{compressType}"
	end
	
	{
		#TODO
		
	}.each do |compressType, args|
		inTime, expTime = args
		define_method("test_#{compressType.name.split('::')[-1]}") do
			assertCompress(inTime,expTime,compressType)
		end
		
	end
	
end







