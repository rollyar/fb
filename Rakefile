require 'rake/extensiontask'

# Rakefile para compilar la extensión fb (Firebird Ruby driver)

task :default => [:compile]

Rake::ExtensionTask.new('fb') do |ext|
  ext.ext_dir = '.'
  ext.lib_dir = '.'
  ext.source_pattern = "*.c"
end

desc "Clean compiled files"
task :clean do
  rm_f Dir['*.o']
  rm_f 'fb.so'
  rm_f 'Makefile'
  rm_f 'mkmf.log'
end

desc "Clean all generated files"
task :clobber => [:clean] do
  rm_f Dir['*.bundle']
end

desc "Build and install the gem locally"
task :install => [:compile] do
  sh "gem build fb.gemspec"
  sh "gem install fb-*.gem --local"
end

desc "Run tests"
task :test => [:compile] do
  ruby "test/FbTestSuite.rb"
end
