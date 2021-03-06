


module Yojimbomb
	
#
# Date/Time handling for Yojimbo
#
#	Time class (w/ std-lib extensions) is used internally, in favor to Date/DateTime due to performance concerns
#
#	Date, Time, and Numeric (epoch, as defined by core Time) are supported parse inputs, as are Strings 	
#
#	explicit support is provided for date/time strings in formats RFC 2822, ISO 8601
#	otherwise, the string is simply passed to Time.parse(...) from the std-lib
#
#	support for RFC 2616/1123 is NOT provided, b/c RFC 2822 is nearly equivalent, and the ruby std-lib implementation
#		at time of writing is WAY less fiddly
#	
#
module DateTime

	class InvalidDateTime < StandardError
		
	end
	
	class InvalidTimeCompression < StandardError
		
	end
	
	DaysOfWeek = [:sun, :mon, :tue, :wed, :thr, :fri, :sat].freeze
	
	#RFC 2822
	#	day-of-week, DD month-name CCYY hh:mm:ss zone
	#	note that this is also mostly equivalent to RFC 1123/2616 dates, we only support RFC2822 b/c it is ultimatly ??? 
	RFC2822_DOW = ['sun','mon','tue','wed', 'thu', 'fri', 'sat', 'sun'].freeze
	RFC2822_MOY = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"].freeze
	RFC2822_TZD = [
		['UT', 'UTC', 'GMT'],
		['EST', 'EDT', 'CST', 'CDT', 'MST', 'MDT', 'PST', 'PDT'],
		('A'..'Z').to_a - ['J'],
		/(?:[+]|[-])\d{4,4}/
	].flatten.freeze
	RFC822_Pattern = Regexp.new([
		'^',
		/(?:#{RFC2822_DOW.join('|')}\s*[,]\s*)?/,
		[
			/\d\d?/,
			/(?:#{RFC2822_MOY.join('|')})/,
			/\d{4,4}/,
			/\d\d[:]\d\d(?:[:]\d\d)?/,
			/(?:#{RFC2822_TZD.join('|')})/
		].join('\\s+'),
		'$'
	].join(''), Regexp::IGNORECASE)

	#ISO 8601
	# CCYY-MM-DDThh:mm:ss.sss?TZD
	ISO8601_Pattern = /^\d{4,4}[-]\d\d[-]\d\dT\d\d[:]\d\d(?:[:]\d\d(?:[.]\d{1,3})?)?(?:Z|(?:[+]|[-]\d\d[:]\d\d))?$/i
	
	#parses a raw input for occurence bounding into a std-lib enhanced Time object.
	#	we purposefully choose to use Time over Date/DateTime for purposes of performance
	def self.parseTime(raw)
		raretime = case raw
			when Date then raw.to_time
			when Time then raw
			when Numeric then Time.at(raw.to_i)
				
			when String then case raw.strip
				when RFC2822_Pattern then Time.rfc2822(raw.strip)
				when ISO8601_Pattern then Time.iso8601(raw.strip)
				else Time.parse(raw.strip)
			end

			else raise InvalidDateTime.new(raw)
		end
		
		Time.at(raretime.to_i).getutc
	end
	
	def self.numericWeekDay(wd)
		res = {}
		i = 0
		Yojimbomb::DateTime::DaysOfWeek.each do |dow|
			res[dow] = i
			i += 1
		end
		res[wd]
	end
	
	def self.dayOfWeek(time, zone_offset = nil)
		ztime = time.getutc
		ztime = adjTime.localtime(zone_offset) unless zone_offset.nil?
		
		table = {}
		i = 0
		Yojimbomb::DateTime::DaysOfWeek.each do |dow|
			table[i] = dow
			i += 1
		end
		
		table[ztime.wday]
	end
	
	def self.todValue(hour, minOfHour)
		raise :invalidTodValue if hour < 0
		raise :invalidTodValue if minOfHour < 0
		raise :invalidTodValue if hour.to_i > 23
		raise :invalidTodValue if minOfHour.to_i > 59
		
		minFloat = (100 * minOfHour.to_i / 60).to_f / 100.0
		minFloat -= minFloat % 0.01
		res = hour.to_f + minFloat
		res > 23.99 ? 23.99 : res
	end
	
	def self.timeOfDay(time, zone_offset = nil)
		ztime = time.getutc
		ztime = ztime.localtime(zone_offset) unless zone_offset.nil?
		Yojimbomb::DateTime.todValue(ztime.hour, ztime.min)
	end
	
	def self.changeToTimeOfDay(time, tod)
		adjTime = time
		adjTime = Time.at(time.to_i).localtime(time.utc_offset) if (time.to_f % 1) > 0

		minAmt = 60
		hourAmt = 60 * minAmt
		sodDiff = (adjTime.hour * hourAmt) + (adjTime.min * minAmt) + adjTime.sec
		sod = adjTime - sodDiff
		
		hours = tod.to_i
		minPercent = ((tod.to_f % 1.0) * 100.0).to_i.abs
		mins = (60 * minPercent).to_i / 100
		sod + (hours * hourAmt) + (mins * minAmt)
	end
	
	def self.daysBetween(ta, tb)
		(ta - tb).to_i.abs / (60 * 60 * 24)
	end
	
	class TimeCompress
		
		def self.apply(time)
			self.new().compress(time)
		end
		
		def compress(time)
			xtime = time.getutc
			self.onCompress(time)
		end
		
		def onCompress(time)
			#todo: override in subtypes
			throw InvalidTimeCompression('undefined')
		end
		
		def rounding(base,inc)
			factor = base / inc
			round = inc*factor
			diff = base - round
			change = if diff >= 5
				inc
			elsif diff <= -5
				-1*inc
			else
				0
			end
			round + change - base
		end

	end
	
	# combine multiple TimeCompress strategies, applied in order
	class CompositeTimeCompress < TimeCompress
		def initialize(*delegates)
			@delegates = delegates
		end
		
		def onCompress(time)
			xtime = time
			@delegates.each {|delegate| xtime = delegate(xtime)}
			xtime
		end
	end
	
	# round to nearest 1/4 min (15 sec increment)
	class QuarterMinCompress < TimeCompress
		def onCompress(time)
			sec = rounding(time.sec,15)
			time + sec
		end
	end
	
	# round to nearst 1/2 min (30 sec increment)
	class HalfMinCompress < TimeCompress
		def onCompress(time)
			sec = rounding(time.sec,30)
			time + sec
		end
	end
	
	# round to the start of min, discarding sec of min
	class StartOfMinCompress < QuarterMinCompress
		def onCompress(time)
			xtime = super(time)
			xsecs = xtime.sec
			ch = (xsecs >= 45) ? 1 : 0
			xtime + (ch * 60) - xsecs
		end
	end
	
	# round to nearest 15 min increment of hour
	class QuarterHourCompress < StartOfMinCompress
		def onCompress(time)
			xtime = super(time)
			sec = rounding(xtime.min,15) * 60
			time + sec
		end
	end
	
	#round to nearest half hour
	class HalfHourCompress < StartOfMinCompress
		def onCompress(time)
			xtime = super(time)
			sec = rounding(xtime.min,30) * 60
			time + sec
		end
	end
	
	# round to start of hour
	class StartOfHourCompress < QuarterHourCompress
		def onCompress(time)
			xtime = super(time)
			xmins = xtime.min
			ch = (xmins >= 45) ? 1 : 0
			xtime + (ch * 60 * 60) - (xmins * 60)
		end
	end
	
	#round to half day (AM,PM)
	#	where we round to earliest hour of the calculated half of the day,
	#	where 1st half runs from 0 -> 11, & 2nd half runs from 12-> midnight
	#	if time of day is >= 23:45, instead round to next day
	#	min of hour, sec of min not preserved
	class HalfDayCompress < StartOfMinCompress
		def onCompress(time)
			xtime = super(time)
			timeOfDay = (xtime.hour* 100) + xtime.min
			ch = (timeOfDay >= 2345) ? 1 : 0
			hour = case xtime.hour
				when (0..11) then 0
				when (12..23) then 12
			end
			Time.utc(xtime.year, xtime.month, xtime.day, hour, 0, 0)
		end
	end
	
	# round date/time to start of day.
	#	if time of day is >= 23:45; then instead round to next day
	#	does NOT preserve time of day
	class StartOfDayCompress < StartOfMinCompress
		def onCompress(time)
			xtime = super(time)
			timeOfDay = (xtime.hour* 100) + xtime.min
			ch = (timeOfDay >= 2345) ? 1 : 0
			Time.utc(xtime.year, xtime.month, xtime.day, 0, 0, 0) + (ch * 60 * 60 * 24)
		end
	end
	
	# round date/time to 1st day of month
	#	time of day info will be preserved
	class StartOfMonthCompress < TimeCompress
		def onCompress(time)
			Time.utc(time.year,time.month, 1, time.hour, time.min, time.sec)
		end
	end
	
	# round date/time to quarter of year (q1,q2,q3,q4)
	#	this will set month, day-of-month to 1st day of 1st month of determined quarter
	#	time of day info will be preserved
	class QuarterOfYearCompress < TimeCompress
		def onCompress(time)
			month = case time.month
				when (1..3) then 1
				when (4..6) then 4
				when (7..9) then 7
				when (10..12) then 10
			end
			Time.utc(time.year, month, 1, time.hour, time.min, time.sec)
		end
	end
	
end end
