#!/usr/bin/env ruby
# = Windows
# === Sample of Makefile creation:
# <tt>ruby extconf.rb --with-opt-dir=C:/Progra~1/Firebird/Firebird_2_5</tt>
# === Notes
# * Windows is known to build with Ruby from rubyinstaller.org.
# * New in this release is automatically finding your Firebird install under Program Files.
# * If your install is some place non-standard (or on a non-English version of Windows), you'll need to run extconf.rb manually as above.
# * mkmf doesn't like directories with spaces, hence the 8.3 notation in the example above.
# = Linux
# === Notes
# * Build seems to "just work."
# * Unit tests take about 10 times as long to complete using Firebird Classic.  Default xinetd.conf settings may not allow the tests to complete due to the frequency with which new attachments are made.
# = Mac OS X (Intel)
# * Works
require "mkmf"

# Detectar la plataforma
case RUBY_PLATFORM
when /darwin/ # macOS
  firebird_framework = "/Library/Frameworks/Firebird.framework"
  unless Dir.exist?(firebird_framework)
    abort "Error: No se encontró Firebird en /Library/Frameworks/Firebird.framework. Asegúrate de instalar Firebird."
  end

  $CFLAGS += " -DOS_UNIX"
  $CPPFLAGS += " -I#{firebird_framework}/Headers"
  $LDFLAGS += " -L#{firebird_framework}/Libraries -lfbclient"
when /linux/
  $CFLAGS += " -DOS_UNIX"
  $CPPFLAGS += " -I/usr/include/firebird"
  $LDFLAGS += " -L/usr/lib -lfbclient"
when /(mingw32|mswin32|x64-mingw-ucrt)/ # Windows
  $CFLAGS += " -DOS_WIN32"
  firebird_path = ENV["FIREBIRD"] || Dir["C:/Program Files/Firebird/Firebird_*"].sort.last
  unless firebird_path && File.exist?(firebird_path)
    abort "Error: No se encontró Firebird en C:/Program Files/Firebird/"
  end
  $CPPFLAGS += " -I#{firebird_path}/include"
  $LDFLAGS += " -L#{firebird_path}/lib -lfbclient"
end

# Verificar si se puede encontrar libfbclient
unless have_library("fbclient", "isc_attach_database")
  abort "Error: No se encontró la biblioteca libfbclient. Asegúrate de instalar Firebird."
end

create_makefile("fb/fb_ext")
