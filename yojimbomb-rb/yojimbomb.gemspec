DESC = <<-EOD
Yojimbomb Metrics library for Ruby.
Yojimbomb is designed to support a wide variety of different types of metrics,
backed by a variety of persistence solutions.
EOD

Gem::Specification.new do |gem|
	gem.authors = 'Sabras Soft LLC'
	gem.name = 'yojimbomb'
	gem.version = '1.0.0'
	gem.date = Date.today.to_s
	gem.summary = 'Sabras Yojimbomb: Flexible Metrics Data'
	gem.description = DESC
	gem.files = Dir[
		'lib/**/*',
	]
	gem.license = 'MIT'
end