#!/usr/bin/env ruby
# frozen_string_literal: true

# ============================================================================
# SCRIPT DE APLICACIÓN DE PARCHE PARA SOPORTE RETURNING
# ============================================================================
# Este script modifica el archivo fb.c para agregar soporte a 
# INSERT/UPDATE/DELETE ... RETURNING
#
# Uso:
#   ruby apply_returning_patch.rb /path/to/fb.c
#
# Author: Generated for activerecord-fb-adapter
# ============================================================================

require 'fileutils'

class ReturningPatch
  PATCH_MARKER = "// PATCH: RETURNING SUPPORT"

  def initialize(file_path)
    @file_path = file_path
    @content = File.read(file_path)
    @modified = false
  end

  def apply!
    check_already_patched!
    backup_original!
    
    insert_helper_functions!
    modify_cursor_execute_withparams!
    modify_cursor_execute2!
    add_ruby_methods!
    
    write_modified_file!
    puts "✓ Parche aplicado exitosamente a #{@file_path}"
    puts "✓ Backup guardado en #{@file_path}.backup"
  end

  private

  def check_already_patched!
    if @content.include?(PATCH_MARKER)
      raise "El archivo ya tiene el parche aplicado"
    end
  end

  def backup_original!
    FileUtils.cp(@file_path, "#{@file_path}.backup")
  end

  # Insertar funciones auxiliares antes de cursor_execute2
  def insert_helper_functions!
    helper_code = <<~C_CODE
      #{PATCH_MARKER}
      
      /* Determina si el tipo de statement es DML (INSERT, UPDATE, DELETE) */
      static int is_dml_statement(long statement_type)
      {
              return (statement_type == isc_info_sql_stmt_insert ||
                      statement_type == isc_info_sql_stmt_update ||
                      statement_type == isc_info_sql_stmt_delete);
      }

      /* Prepara el buffer de salida para recibir datos de RETURNING */
      static void fb_cursor_prepare_output_buffer(struct FbCursor *fb_cursor)
      {
              long cols;
              long count;
              long length;
              long alignment;
              long offset;
              XSQLVAR *var;
              short dtp;

              cols = fb_cursor->o_sqlda->sqld;
              if (cols == 0) return;

              for (var = fb_cursor->o_sqlda->sqlvar, offset = 0, count = 0; 
                   count < cols; var++, count++) {
                      length = alignment = var->sqllen;
                      dtp = var->sqltype & ~1;

                      if (dtp == SQL_TEXT) {
                              alignment = 1;
                      } else if (dtp == SQL_VARYING) {
                              length += sizeof(short);
                              alignment = sizeof(short);
                      }

                      offset = FB_ALIGN(offset, alignment);
                      var->sqldata = (char*)(fb_cursor->o_buffer + offset);
                      offset += length;
                      offset = FB_ALIGN(offset, sizeof(short));
                      var->sqlind = (short*)(fb_cursor->o_buffer + offset);
                      offset += sizeof(short);
              }

              length = calculate_buffsize(fb_cursor->o_sqlda);
              if (length > fb_cursor->o_buffer_size) {
                      fb_cursor->o_buffer = xrealloc(fb_cursor->o_buffer, length);
                      fb_cursor->o_buffer_size = length;
              }
      }

      /* Obtiene los valores retornados de un DML con RETURNING */
      static VALUE fb_cursor_fetch_returning(struct FbCursor *fb_cursor, 
                                              struct FbConnection *fb_connection)
      {
              VALUE result;
              long cols;
              long count;
              XSQLVAR *var;
              long dtp;
              VALUE val;
              VARY *vary;
              struct tm tms;

              if (isc_dsql_fetch(fb_connection->isc_status, &fb_cursor->stmt, 
                                 1, fb_cursor->o_sqlda) != 0) {
                      return Qnil;
              }
              fb_error_check(fb_connection->isc_status);

              cols = fb_cursor->o_sqlda->sqld;
              result = rb_ary_new2(cols);

              for (count = 0; count < cols; count++) {
                      var = &fb_cursor->o_sqlda->sqlvar[count];
                      dtp = var->sqltype & ~1;

                      if ((var->sqltype & 1) && (*var->sqlind < 0)) {
                              val = Qnil;
                      } else {
                              switch (dtp) {
                                      case SQL_TEXT:
                                              val = rb_str_new(var->sqldata, var->sqllen);
                                              #if HAVE_RUBY_ENCODING_H
                                              rb_funcall(val, id_force_encoding, 1, 
                                                         fb_connection->encoding);
                                              #endif
                                              break;

                                      case SQL_VARYING:
                                              vary = (VARY*)var->sqldata;
                                              val = rb_str_new(vary->vary_string, vary->vary_length);
                                              #if HAVE_RUBY_ENCODING_H
                                              rb_funcall(val, id_force_encoding, 1, 
                                                         fb_connection->encoding);
                                              #endif
                                              break;

                                      case SQL_SHORT:
                                              if (var->sqlscale < 0) {
                                                      val = sql_decimal_to_bigdecimal(
                                                              (long long)*(ISC_SHORT*)var->sqldata, 
                                                              var->sqlscale);
                                              } else {
                                                      val = INT2NUM((int)*(short*)var->sqldata);
                                              }
                                              break;

                                      case SQL_LONG:
                                              if (var->sqlscale < 0) {
                                                      val = sql_decimal_to_bigdecimal(
                                                              (long long)*(ISC_LONG*)var->sqldata, 
                                                              var->sqlscale);
                                              } else {
                                                      val = INT2NUM(*(ISC_LONG*)var->sqldata);
                                              }
                                              break;

                                      case SQL_FLOAT:
                                              val = rb_float_new((double)*(float*)var->sqldata);
                                              break;

                                      case SQL_DOUBLE:
                                              val = rb_float_new(*(double*)var->sqldata);
                                              break;

                                      case SQL_INT64:
                                              if (var->sqlscale < 0) {
                                                      val = sql_decimal_to_bigdecimal(
                                                              *(ISC_INT64*)var->sqldata, 
                                                              var->sqlscale);
                                              } else {
                                                      val = LL2NUM(*(ISC_INT64*)var->sqldata);
                                              }
                                              break;

                                      case SQL_TIMESTAMP:
                                              isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &tms);
                                              val = fb_mktime(&tms, "local");
                                              break;

                                      case SQL_TYPE_TIME:
                                              isc_decode_sql_time((ISC_TIME *)var->sqldata, &tms);
                                              tms.tm_year = 100;
                                              tms.tm_mon = 0;
                                              tms.tm_mday = 1;
                                              val = fb_mktime(&tms, "utc");
                                              break;

                                      case SQL_TYPE_DATE:
                                              isc_decode_sql_date((ISC_DATE *)var->sqldata, &tms);
                                              val = fb_mkdate(&tms);
                                              break;

      #if (FB_API_VER >= 30)
                                      case SQL_BOOLEAN:
                                              val = (*(bool*)var->sqldata) ? Qtrue : Qfalse;
                                              break;
      #endif

                                      default:
                                              val = Qnil;
                                              break;
                              }
                      }
                      rb_ary_push(result, val);
              }

              return result;
      }

      /* END PATCH: RETURNING SUPPORT */

    C_CODE

    # Insertar antes de cursor_execute2
    @content.sub!(/static VALUE cursor_execute2/, "#{helper_code}\nstatic VALUE cursor_execute2")
    @modified = true
  end

  # Modificar fb_cursor_execute_withparams
  def modify_cursor_execute_withparams!
    # Buscar la función y reemplazar las llamadas a isc_dsql_execute2
    old_pattern = /static void fb_cursor_execute_withparams\(struct FbCursor \*fb_cursor, long argc, VALUE \*argv\)\s*\{.*?^}/m

    new_function = <<~C_CODE
      static void fb_cursor_execute_withparams(struct FbCursor *fb_cursor, long argc, VALUE *argv)
      {
              struct FbConnection *fb_connection;
              long statement_type = 0;
              char isc_info_buff[16];
              char isc_info_stmt[] = { isc_info_sql_stmt_type };
              long length;
              int has_returning;

              TypedData_Get_Struct(fb_cursor->connection, struct FbConnection, 
                                    &fbconnection_data_type, fb_connection);

              /* Obtener tipo de statement */
              isc_dsql_sql_info(fb_connection->isc_status, &fb_cursor->stmt,
                                sizeof(isc_info_stmt), isc_info_stmt,
                                sizeof(isc_info_buff), isc_info_buff);
              
              if (isc_info_buff[0] == isc_info_sql_stmt_type) {
                      length = isc_vax_integer(&isc_info_buff[1], 2);
                      statement_type = isc_vax_integer(&isc_info_buff[3], (short)length);
              }

              /* Verificar si es DML con RETURNING */
              has_returning = is_dml_statement(statement_type) && 
                              (fb_cursor->o_sqlda->sqld > 0);

              if (argc >= 1 && TYPE(argv[0]) == T_ARRAY) {
                      int i;
                      VALUE obj;
                      VALUE ary = argv[0];
                      if (RARRAY_LEN(ary) > 0 && TYPE(RARRAY_PTR(ary)[0]) == T_ARRAY) {
                              for (i = 0; i < RARRAY_LEN(ary); i++) {
                                      obj = rb_ary_entry(ary, i);
                                      fb_cursor_execute_withparams(fb_cursor, 1, &obj);
                              }
                      } else {
                              for (i = 0; i < argc; i++) {
                                      obj = argv[i];
                                      Check_Type(obj, T_ARRAY);
                                      fb_cursor_set_inputparams(fb_cursor, RARRAY_LEN(obj), 
                                                                RARRAY_PTR(obj));

                                      if (has_returning) {
                                              fb_cursor_prepare_output_buffer(fb_cursor);
                                              isc_dsql_execute2(fb_connection->isc_status, 
                                                               &fb_connection->transact, 
                                                               &fb_cursor->stmt, 
                                                               SQLDA_VERSION1, 
                                                               fb_cursor->i_sqlda, 
                                                               fb_cursor->o_sqlda);
                                      } else {
                                              isc_dsql_execute2(fb_connection->isc_status, 
                                                               &fb_connection->transact, 
                                                               &fb_cursor->stmt, 
                                                               SQLDA_VERSION1, 
                                                               fb_cursor->i_sqlda, 
                                                               NULL);
                                      }
                                      fb_error_check(fb_connection->isc_status);
                              }
                      }
              } else {
                      fb_cursor_set_inputparams(fb_cursor, argc, argv);

                      if (has_returning) {
                              fb_cursor_prepare_output_buffer(fb_cursor);
                              isc_dsql_execute2(fb_connection->isc_status, 
                                               &fb_connection->transact, 
                                               &fb_cursor->stmt, 
                                               SQLDA_VERSION1, 
                                               fb_cursor->i_sqlda, 
                                               fb_cursor->o_sqlda);
                      } else {
                              isc_dsql_execute2(fb_connection->isc_status, 
                                               &fb_connection->transact, 
                                               &fb_cursor->stmt, 
                                               SQLDA_VERSION1, 
                                               fb_cursor->i_sqlda, 
                                               NULL);
                      }
                      fb_error_check(fb_connection->isc_status);
              }
      }
    C_CODE

    @content.sub!(old_pattern, new_function)
    @modified = true
  end

  # Modificar cursor_execute2
  def modify_cursor_execute2!
    old_pattern = /static VALUE cursor_execute2\(VALUE args\)\s*\{.*?^}/m

    new_function = <<~C_CODE
      static VALUE cursor_execute2(VALUE args)
      {
              struct FbCursor *fb_cursor;
              struct FbConnection *fb_connection;
              char *sql;
              VALUE rb_sql;
              long statement_type;
              long length;
              long in_params;
              long cols;
              long rows_affected;
              VALUE result = Qnil;
              char isc_info_buff[16];
              char isc_info_stmt[] = { isc_info_sql_stmt_type };
              int is_dml_with_returning = 0;

              VALUE self = rb_ary_pop(args);
              TypedData_Get_Struct(self, struct FbCursor, &fbcursor_data_type, fb_cursor);
              TypedData_Get_Struct(fb_cursor->connection, struct FbConnection, 
                                    &fbconnection_data_type, fb_connection);

              rb_sql = rb_ary_shift(args);
              sql = StringValuePtr(rb_sql);

              /* Prepare query */
              isc_dsql_prepare(fb_connection->isc_status, &fb_connection->transact, 
                               &fb_cursor->stmt, 0, sql, 
                               fb_connection_dialect(fb_connection), 
                               fb_cursor->o_sqlda);
              fb_error_check(fb_connection->isc_status);

              /* Get the statement type */
              isc_dsql_sql_info(fb_connection->isc_status, &fb_cursor->stmt,
                                sizeof(isc_info_stmt), isc_info_stmt,
                                sizeof(isc_info_buff), isc_info_buff);
              fb_error_check(fb_connection->isc_status);

              if (isc_info_buff[0] == isc_info_sql_stmt_type) {
                      length = isc_vax_integer(&isc_info_buff[1], 2);
                      statement_type = isc_vax_integer(&isc_info_buff[3], (short)length);
              } else {
                      statement_type = 0;
              }

              /* Describe the parameters */
              isc_dsql_describe_bind(fb_connection->isc_status, &fb_cursor->stmt, 
                                     1, fb_cursor->i_sqlda);
              fb_error_check(fb_connection->isc_status);

              /* Describe output columns */
              isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 
                                1, fb_cursor->o_sqlda);
              fb_error_check(fb_connection->isc_status);

              /* Get the number of parameters and reallocate the SQLDA */
              in_params = fb_cursor->i_sqlda->sqld;
              if (fb_cursor->i_sqlda->sqln < in_params) {
                      xfree(fb_cursor->i_sqlda);
                      fb_cursor->i_sqlda = sqlda_alloc(in_params);
                      isc_dsql_describe_bind(fb_connection->isc_status, &fb_cursor->stmt, 
                                             1, fb_cursor->i_sqlda);
                      fb_error_check(fb_connection->isc_status);
              }

              /* Get the size of parameters buffer and reallocate it */
              if (in_params) {
                      length = calculate_buffsize(fb_cursor->i_sqlda);
                      if (length > fb_cursor->i_buffer_size) {
                              fb_cursor->i_buffer = xrealloc(fb_cursor->i_buffer, length);
                              memset(fb_cursor->i_buffer, 0, length);
                              fb_cursor->i_buffer_size = length;
                      }
              }

              /* DETERMINAR SI ES DML CON RETURNING */
              is_dml_with_returning = is_dml_statement(statement_type) && 
                                      (fb_cursor->o_sqlda->sqld > 0);

              /* CASO 1: DML CON RETURNING */
              if (is_dml_with_returning) {
                      cols = fb_cursor->o_sqlda->sqld;
                      if (fb_cursor->o_sqlda->sqln < cols) {
                              xfree(fb_cursor->o_sqlda);
                              fb_cursor->o_sqlda = sqlda_alloc(cols);
                              isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 
                                                1, fb_cursor->o_sqlda);
                              fb_error_check(fb_connection->isc_status);
                      }

                      if (in_params) {
                              fb_cursor_set_inputparams(fb_cursor, RARRAY_LEN(args), 
                                                        RARRAY_PTR(args));
                      }

                      fb_cursor_prepare_output_buffer(fb_cursor);

                      /* EJECUTAR CON OUTPUT SQLDA - CLAVE PARA RETURNING */
                      isc_dsql_execute2(fb_connection->isc_status, 
                                       &fb_connection->transact, 
                                       &fb_cursor->stmt, 
                                       SQLDA_VERSION1, 
                                       in_params ? fb_cursor->i_sqlda : NULL, 
                                       fb_cursor->o_sqlda);
                      fb_error_check(fb_connection->isc_status);

                      fb_cursor->open = Qtrue;

                      fb_cursor->fields_ary = fb_cursor_fields_ary(fb_cursor->o_sqlda, 
                                                                   fb_connection->downcase_names);
                      fb_cursor->fields_hash = fb_cursor_fields_hash(fb_cursor->fields_ary);

                      result = fb_cursor_fetch_returning(fb_cursor, fb_connection);

                      isc_dsql_free_statement(fb_connection->isc_status, 
                                              &fb_cursor->stmt, DSQL_close);
                      fb_error_check(fb_connection->isc_status);
                      fb_cursor->open = Qfalse;

                      if (NIL_P(result)) {
                              result = rb_ary_new();
                      }

                      rows_affected = cursor_rows_affected(fb_cursor, statement_type);
                      
                      VALUE hash_result = rb_hash_new();
                      rb_hash_aset(hash_result, ID2SYM(rb_intern("returning")), result);
                      rb_hash_aset(hash_result, ID2SYM(rb_intern("rows_affected")), 
                                   INT2NUM((int)rows_affected));
                      
                      return hash_result;
              }

              /* CASO 2: DML SIN RETURNING (comportamiento original) */
              if (!fb_cursor->o_sqlda->sqld) {
                      if (statement_type == isc_info_sql_stmt_start_trans) {
                              rb_raise(rb_eFbError, "use Fb::Connection#transaction()");
                      } else if (statement_type == isc_info_sql_stmt_commit) {
                              rb_raise(rb_eFbError, "use Fb::Connection#commit()");
                      } else if (statement_type == isc_info_sql_stmt_rollback) {
                              rb_raise(rb_eFbError, "use Fb::Connection#rollback()");
                      } else if (in_params) {
                              fb_cursor_execute_withparams(fb_cursor, RARRAY_LEN(args), 
                                                           RARRAY_PTR(args));
                      } else {
                              isc_dsql_execute2(fb_connection->isc_status, 
                                               &fb_connection->transact, 
                                               &fb_cursor->stmt, 
                                               SQLDA_VERSION1, NULL, NULL);
                              fb_error_check(fb_connection->isc_status);
                      }
                      rows_affected = cursor_rows_affected(fb_cursor, statement_type);
                      result = INT2NUM((int)rows_affected);
              } else {
                      /* CASO 3: SELECT (comportamiento original) */
                      cols = fb_cursor->o_sqlda->sqld;
                      if (fb_cursor->o_sqlda->sqln < cols) {
                              xfree(fb_cursor->o_sqlda);
                              fb_cursor->o_sqlda = sqlda_alloc(cols);
                              isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 
                                                1, fb_cursor->o_sqlda);
                              fb_error_check(fb_connection->isc_status);
                      }

                      if (in_params) {
                              fb_cursor_set_inputparams(fb_cursor, RARRAY_LEN(args), 
                                                        RARRAY_PTR(args));
                      }

                      isc_dsql_execute2(fb_connection->isc_status, 
                                       &fb_connection->transact, 
                                       &fb_cursor->stmt, 
                                       SQLDA_VERSION1, 
                                       in_params ? fb_cursor->i_sqlda : NULL, 
                                       NULL);
                      fb_error_check(fb_connection->isc_status);
                      fb_cursor->open = Qtrue;

                      length = calculate_buffsize(fb_cursor->o_sqlda);
                      if (length > fb_cursor->o_buffer_size) {
                              fb_cursor->o_buffer = xrealloc(fb_cursor->o_buffer, length);
                              fb_cursor->o_buffer_size = length;
                      }

                      fb_cursor->fields_ary = fb_cursor_fields_ary(fb_cursor->o_sqlda, 
                                                                   fb_connection->downcase_names);
                      fb_cursor->fields_hash = fb_cursor_fields_hash(fb_cursor->fields_ary);
              }
              return result;
      }
    C_CODE

    @content.sub!(old_pattern, new_function)
    @modified = true
  end

  # Agregar métodos Ruby
  def add_ruby_methods!
    # Buscar Init_fb y agregar los nuevos métodos
    init_section = <<~C_CODE
      
              /* PATCH: Métodos para RETURNING */
              rb_define_method(rb_cFbConnection, "execute_returning", 
                               connection_execute_returning, -1);
              rb_define_method(rb_cFbConnection, "insert_returning", 
                               connection_insert_returning, -1);

      }

    C_CODE

    @content.sub!(/void Init_fb\(void\)\s*\{.*?^}/m) do |match|
      # Agregar las definiciones de funciones antes de Init_fb
      func_defs = <<~C_CODE
        
        /* PATCH: Nuevos métodos Ruby para RETURNING */
        static VALUE connection_execute_returning(int argc, VALUE *argv, VALUE self)
        {
                VALUE cursor = connection_cursor(self);
                VALUE val = cursor_execute(argc, argv, cursor);
                cursor_close(cursor);
                return val;
        }

        static VALUE connection_insert_returning(int argc, VALUE *argv, VALUE self)
        {
                VALUE result = connection_execute_returning(argc, argv, self);
                
                if (TYPE(result) == T_HASH) {
                        VALUE returning = rb_hash_aref(result, ID2SYM(rb_intern("returning")));
                        if (TYPE(returning) == T_ARRAY && RARRAY_LEN(returning) > 0) {
                                return rb_ary_entry(returning, 0);
                        }
                }
                
                return Qnil;
        }

      C_CODE

      # Insertar antes de Init_fb
      match + func_defs
    end

    @modified = true
  end

  def write_modified_file!
    File.write(@file_path, @content)
  end
end

# Main execution
if __FILE__ == $PROGRAM_NAME
  if ARGV.empty?
    puts "Uso: ruby #{$PROGRAM_NAME} /path/to/fb.c"
    exit 1
  end

  file_path = ARGV[0]

  unless File.exist?(file_path)
    puts "Error: El archivo #{file_path} no existe"
    exit 1
  end

  begin
    patcher = ReturningPatch.new(file_path)
    patcher.apply!
  rescue => e
    puts "Error: #{e.message}"
    exit 1
  end
end
