# frozen_string_literal: true

require "mkmf"

case RUBY_PLATFORM
when /darwin/ # macOS
  framework = "/Library/Frameworks/Firebird.framework"
  unless Dir.exist?(framework)
    abort <<~MSG
      Error: Firebird not found.
      Donwload and install Firebird from:
        https://firebirdsql.org/en/server-packages/
      The setup set Firebird in #{framework}
    MSG
  end
  $CFLAGS << " -DOS_UNIX"
  $CPPFLAGS << " -I#{framework}/Headers"
  $LDFLAGS << " -L#{framework}/Libraries -lfbclient"

when /linux/
  $CFLAGS << " -DOS_UNIX"
  $CPPFLAGS << " -I/usr/include/firebird"
  $LDFLAGS << " -L/usr/lib -lfbclient"

when /(mingw|mswin)/
  $CFLAGS << " -DOS_WIN32"
  fb_root = ENV["FIREBIRD"] ||
            Dir["C:/Program Files/Firebird/Firebird_*"].max ||
            Dir["C:/Program Files (x86)/Firebird/Firebird_*"].max
  unless fb_root && File.directory?(fb_root)
    abort "Firebird not found in windows. Use the variable FIREBIRD."
  end
  $CPPFLAGS << " -I#{fb_root}/include"
  $LDFLAGS << " -L#{fb_root}/lib -lfbclient"

else
  abort "Not supported plataform: #{RUBY_PLATFORM}"
end

# Verificaciones
unless find_header("ibase.h")
  abort "ibase.h not found. ¿Is Firebird installed?"
end

unless have_library("fbclient", "isc_attach_database")
  abort "Can't link with libfbclient."
end

create_makefile("fb/fb_ext")


# frozen_string_literal: true

require "mkmf"

# Detectar sistema operativo
is_macos = RUBY_PLATFORM =~ /darwin/
is_linux = RUBY_PLATFORM =~ /linux/
is_windows = RUBY_PLATFORM =~ /(mingw|mswin)/

# Configuración por defecto
firebird_includes = nil
firebird_libs = nil

if is_macos
  # macOS: Firebird suele instalarse como framework
  framework_path = "/Library/Frameworks/Firebird.framework"
  if Dir.exist?(framework_path)
    firebird_includes = "#{framework_path}/Headers"
    firebird_libs = "#{framework_path}/Libraries"
    $CFLAGS << " -DOS_UNIX"
  else
    # Check for Homebrew
    homebrew_path = "/opt/homebrew" # Apple Silicon
    homebrew_path = "/usr/local" if File.exist?("/usr/local/include") # Intel
    if Dir.exist?("#{homebrew_path}/include/firebird")
      firebird_includes = "#{homebrew_path}/include/firebird"
      firebird_libs = "#{homebrew_path}/lib"
      $CFLAGS << " -DOS_UNIX"
    else
      abort "Error: No se encontró Firebird en macOS. Instala Firebird desde https://firebirdsql.org o con Homebrew (`brew install firebird`)."
    end
  end

elsif is_linux
  # Linux: rutas estándar de paquetes (Debian/Ubuntu, RHEL, etc.)
  $CFLAGS << " -DOS_UNIX"
  firebird_includes = "/usr/include/firebird"
  firebird_libs = "/usr/lib"

elsif is_windows
  $CFLAGS << " -DOS_WIN32"
  # Buscar en variables de entorno o rutas comunes
  firebird_root = ENV["FIREBIRD"] ||
                  Dir.glob("C:/Program Files/Firebird/Firebird_*").sort.last ||
                  Dir.glob("C:/Program Files (x86)/Firebird/Firebird_*").sort.last

  unless firebird_root && File.directory?(firebird_root)
    abort "Error: No se encontró Firebird en Windows. Establece la variable FIREBIRD o instala Firebird en 'C:/Program Files/Firebird/'."
  end

  firebird_includes = "#{firebird_root}/include"
  firebird_libs = "#{firebird_root}/lib"

else
  abort "Plataforma no soportada: #{RUBY_PLATFORM}"
end

# Aplicar rutas encontradas
$CPPFLAGS << " -I#{firebird_includes}" if firebird_includes
$LDFLAGS << " -L#{firebird_libs}" if firebird_libs

# Verificar que se pueda enlazar con fbclient
unless have_library("fbclient", "isc_attach_database")
  abort "Error: No se pudo enlazar con libfbclient. Asegúrate de que Firebird esté instalado y las rutas sean correctas."
end

# Opcional: verificar que los headers existan
unless find_header("ibase.h", firebird_includes)
  abort "Error: No se encontró 'ibase.h'. Verifica la ruta de inclusión de Firebird."
end

# Generar Makefile
create_makefile("fb/fb_ext")
