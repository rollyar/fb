# Soporte INSERT/UPDATE/DELETE ... RETURNING para activerecord-fb-adapter

## Resumen del Problema

El adaptador original **NO soporta correctamente** la cláusula `RETURNING` para sentencias DML (INSERT, UPDATE, DELETE) en Firebird 2.0+. El problema radica en que:

1. **Nunca pasa el output SQLDA** a `isc_dsql_execute2`
2. **No detecta** cuando un DML tiene columnas de salida
3. **No retorna** los valores generados por RETURNING

## Archivos Incluidos

| Archivo | Descripción |
|---------|-------------|
| `fb_returning_patch.c` | Código de las funciones modificadas/nuevas |
| `fb_full_modified.c` | Versión completa del archivo fb.c con modificaciones |
| `INTEGRATION_GUIDE.md` | Este archivo - guía de integración |

## Cambios Realizados

### 1. Nueva Función: `is_dml_statement()`
```c
static int is_dml_statement(long statement_type)
```
Determina si el tipo de statement es INSERT, UPDATE o DELETE.

### 2. Nueva Función: `fb_cursor_prepare_output_buffer()`
```c
static void fb_cursor_prepare_output_buffer(struct FbCursor *fb_cursor)
```
Prepara el buffer de salida para recibir los valores de RETURNING.

### 3. Nueva Función: `fb_cursor_fetch_returning()`
```c
static VALUE fb_cursor_fetch_returning(struct FbCursor *fb_cursor, 
                                        struct FbConnection *fb_connection)
```
Obtiene los valores retornados de un DML con RETURNING.

### 4. Función Modificada: `cursor_execute2()`
Detecta automáticamente DML con RETURNING y:
- Pasa el output SQLDA correctamente a `isc_dsql_execute2`
- Ejecuta fetch para obtener los valores
- Retorna un hash con `:returning` y `:rows_affected`

### 5. Función Modificada: `fb_cursor_execute_withparams()`
Soporte para ejecución batch con RETURNING.

## Instrucciones de Integración

### Opción A: Parche Manual

1. **Abrir el archivo `fb.c` original**

2. **Agregar las funciones auxiliares** antes de `cursor_execute2` (aproximadamente línea 2100):
   ```c
   // Insertar las funciones:
   // - is_dml_statement()
   // - fb_cursor_prepare_output_buffer()
   // - fb_cursor_fetch_returning()
   ```

3. **Reemplazar la función `cursor_execute2`** con la versión modificada

4. **Reemplazar la función `fb_cursor_execute_withparams`** con la versión modificada

5. **Agregar nuevos métodos Ruby** en la función `Init_fb()`:
   ```c
   // Buscar esta sección al final del archivo
   rb_define_method(rb_cFbConnection, "execute_returning", 
                    connection_execute_returning, -1);
   rb_define_method(rb_cFbConnection, "insert_returning", 
                    connection_insert_returning, -1);
   ```

6. **Recompilar la gema**:
   ```bash
   cd /path/to/activerecord-fb-adapter
   rake compile
   # o
   gem build activerecord-fb-adapter.gemspec
   gem install *.gem
   ```

### Opción B: Reemplazo Completo

1. **Backup del archivo original**:
   ```bash
   cp fb.c fb.c.backup
   ```

2. **Reemplazar con el archivo modificado**:
   ```bash
   cp fb_full_modified.c fb.c
   ```

3. **Recompilar**:
   ```bash
   rake compile
   ```

## Uso de la API

### Ejemplo Básico: INSERT con RETURNING

```ruby
require 'fb'

Fb::Database.connect(
  database: 'localhost:/var/lib/firebird/test.fdb',
  username: 'sysdba',
  password: 'masterkey'
) do |conn|
  
  # Método 1: execute retorna hash
  result = conn.execute(
    "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id, created_at",
    "John Doe", "john@example.com"
  )
  puts result[:returning]       # => [1, "2024-01-15 10:30:00"]
  puts result[:rows_affected]   # => 1
  
  # Método 2: insert_returning retorna solo el ID
  id = conn.insert_returning(
    "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
    "Jane Doe", "jane@example.com"
  )
  puts id  # => 2
  
end
```

### UPDATE con RETURNING

```ruby
result = conn.execute(
  "UPDATE users SET last_login = CURRENT_TIMESTAMP 
   WHERE email = ? RETURNING id, name, last_login",
  "john@example.com"
)
puts result[:returning]  # => [1, "John Doe", "2024-01-15 10:35:00"]
```

### DELETE con RETURNING

```ruby
result = conn.execute(
  "DELETE FROM users WHERE id = ? RETURNING name, email",
  1
)
puts result[:returning]  # => ["John Doe", "john@example.com"]
```

### Integración con ActiveRecord

Modificar `lib/active_record/connection_adapters/firebird_adapter.rb`:

```ruby
module ActiveRecord
  module ConnectionAdapters
    class FirebirdAdapter < AbstractAdapter
      
      # Sobrescribir el método insert
      def insert(arel, name = nil, pk = nil, id_value = nil, sequence_name = nil, binds = [])
        sql, binds = to_sql_and_binds(arel, binds)
        
        if pk && supports_insert_returning?
          sql = "#{sql} RETURNING #{quote_column_name(pk)}"
          result = @connection.execute(sql, *bind_values(binds))
          result[:returning]&.first
        else
          @connection.execute(sql, *bind_values(binds))
        end
      end
      
      # Método para INSERT con RETURNING
      def insert_returning(sql, binds = [], returning_columns = nil)
        if returning_columns
          returning_clause = Array(returning_columns).map { |c| quote_column_name(c) }.join(', ')
          sql = "#{sql} RETURNING #{returning_clause}"
        end
        
        result = @connection.execute(sql, *bind_values(binds))
        result[:returning]
      end
      
      def supports_insert_returning?
        true
      end
      
    end
  end
end
```

## Compatibilidad

| Firebird Version | RETURNING Support |
|------------------|-------------------|
| 1.5.x | ❌ No soportado |
| 2.0 | ✅ INSERT |
| 2.1 | ✅ INSERT, UPDATE |
| 2.5 | ✅ INSERT, UPDATE, DELETE |
| 3.0+ | ✅ Completo |

## Pruebas

Crear archivo `test/test_returning.rb`:

```ruby
require 'test/unit'
require 'fb'

class TestReturning < Test::Unit::TestCase
  
  def setup
    @db = Fb::Database.new(
      database: 'localhost:/tmp/test_returning.fdb',
      username: 'sysdba',
      password: 'masterkey'
    )
    @db.create do |conn|
      conn.execute <<-SQL
        CREATE TABLE users (
          id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          email VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      SQL
    end
    @conn = @db.connect
  end
  
  def teardown
    @conn.close if @conn
    @db.drop
  end
  
  def test_insert_returning_single_column
    result = @conn.execute(
      "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
      "Test User", "test@example.com"
    )
    
    assert_equal 1, result[:rows_affected]
    assert_instance_of Array, result[:returning]
    assert_equal 1, result[:returning].length
    assert result[:returning][0] > 0
  end
  
  def test_insert_returning_multiple_columns
    result = @conn.execute(
      "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id, name, created_at",
      "Test User", "test@example.com"
    )
    
    assert_equal 3, result[:returning].length
    assert result[:returning][0] > 0  # id
    assert_equal "Test User", result[:returning][1]  # name
    assert_instance_of Time, result[:returning][2]  # created_at
  end
  
  def test_update_returning
    # Insert primero
    @conn.execute(
      "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
      "Original", "original@example.com"
    )
    
    # Update con returning
    result = @conn.execute(
      "UPDATE users SET name = ? WHERE email = ? RETURNING id, name",
      "Updated", "original@example.com"
    )
    
    assert_equal 1, result[:rows_affected]
    assert_equal "Updated", result[:returning][1]
  end
  
  def test_delete_returning
    # Insert primero
    @conn.execute(
      "INSERT INTO users (name, email) VALUES (?, ?)",
      "To Delete", "delete@example.com"
    )
    
    # Delete con returning
    result = @conn.execute(
      "DELETE FROM users WHERE email = ? RETURNING name, email",
      "delete@example.com"
    )
    
    assert_equal 1, result[:rows_affected]
    assert_equal "To Delete", result[:returning][0]
    assert_equal "delete@example.com", result[:returning][1]
  end
  
  def test_insert_returning_method
    id = @conn.insert_returning(
      "INSERT INTO users (name, email) VALUES (?, ?) RETURNING id",
      "Convenience", "convenience@example.com"
    )
    
    assert id > 0
  end
  
end
```

## Troubleshooting

### Error: "Invalid output SQLDA"

**Causa**: El buffer de salida no está correctamente inicializado.

**Solución**: Asegúrate de llamar `fb_cursor_prepare_output_buffer()` antes de `isc_dsql_execute2()`.

### Error: "No data available"

**Causa**: El DML no afectó ninguna fila.

**Solución**: Verificar que la condición WHERE coincide con registros existentes. El resultado `:returning` será un array vacío `[]`.

### Error: "SQLDA version mismatch"

**Causa**: Versión incompatible de SQLDA.

**Solución**: Verificar que `SQLDA_VERSION1` o `SQLDA_CURRENT_VERSION` está definido correctamente.

## Notas Técnicas

1. **¿Por qué `isc_dsql_execute2` y no `isc_dsql_execute`?**
   - `isc_dsql_execute2` permite especificar un output SQLDA que recibe los valores directamente
   - Con `isc_dsql_execute` se necesitaría un fetch adicional que no siempre funciona

2. **¿Por qué retornar un hash?**
   - Permite incluir tanto los valores retornados como el número de filas afectadas
   - Es más informativo para debugging

3. **Rendimiento**
   - RETURNING es más eficiente que SELECT después de INSERT
   - Evita condiciones de carrera
   - Reduce round-trips al servidor

## Licencia

Este parche mantiene la misma licencia que el código original del adaptador.
