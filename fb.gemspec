# frozen_string_literal: true
# -*- encoding: utf-8 -*-

require_relative 'lib/fb/version'

Gem::Specification.new do |spec|
  spec.name     = "fb"
  spec.version  = Fb::VERSION
  spec.author   = "Brent Rowland"
  spec.email    = "rowland@rowlandresearch.com"
  spec.homepage = "http://github.com/rowland/fb"

  spec.date = "2019-06-08"
  spec.summary = "Firebird database driver"
  spec.description = "Ruby Firebird Extension Library"
  spec.license = "MIT"
  spec.requirements = "Firebird client library fbclient.dll, libfbclient.so or Firebird.framework."
  spec.required_ruby_version = ">= 2.5"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/ged/ruby-pg"
  spec.metadata["changelog_uri"] = "https://github.com/ged/ruby-pg/blob/master/History.md"
  spec.metadata["documentation_uri"] = "http://deveiate.org/code/pg"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end

  spec.extensions    = ["ext/extconf.rb"]
  spec.require_paths = ["lib"]
end
