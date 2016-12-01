/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Run Plugin - Runs any executable binary installed and available on the Hadoop nodes.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Run")
@Description("This plugin allows user to run any executable binary installed and available on the local filesystem of" +
  " the Hadoop nodes. Plugin allows the user's executable binary to read the structured record as input and returns " +
  "the output record, to be further processed downstream in the pipeline.")
public class Run extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Run.class);
  private final RunConfig config;
  private Schema outputSchema;
  private Process process;
  private ExternalProgramExecutor executor;

  public Run(RunConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Preconditions.checkArgument(inputSchema != null, "Input Schema must be a known constant.");
    config.validateBinaryExecutableType();
    config.validateInputFields(inputSchema);
    config.validateOutputFieldType();
    stageConfigurer.setOutputSchema(buildOutputSchema(inputSchema));
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    //TODO - name extraction
    executor = new ExternalProgramExecutor("runner", config.commandToExecute);
    executor.start();

  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    outputSchema = outputSchema == null ? buildOutputSchema(structuredRecord.getSchema()) : outputSchema;
    StringBuilder executableCommand = new StringBuilder();

    if (!Strings.isNullOrEmpty(config.fieldsToProcess)) {
      for (String inputField : Splitter.on(',').trimResults().split(config.fieldsToProcess)) {
        Schema inputFieldSchema = structuredRecord.getSchema().getField(inputField).getSchema();
        Schema.Type inputFieldType = inputFieldSchema.isNullable() ? inputFieldSchema.getNonNullable().getType() :
          inputFieldSchema.getType();

        if ((inputFieldType.equals(Schema.Type.STRING)) &&
          ((String) (structuredRecord.get(inputField))).contains(" ")) {
          executableCommand.append("\"");
          executableCommand.append(structuredRecord.get(inputField));
          executableCommand.append("\"");
        } else {
          executableCommand.append(structuredRecord.get(inputField));
        }
      }
    }

    // append one space before adding the fixed arguments
    executableCommand.append(" ");

    // append the fixed arguments in the command, if any
    if (!Strings.isNullOrEmpty(config.fixedArguments)) {
      executableCommand.append(config.fixedArguments);
    }

    //for each structured record
    SettableFuture<String> completion = SettableFuture.create();
    System.out.println("Calling sbmit from transform");
    executor.submit(executableCommand.toString(), completion, emitter, structuredRecord, outputSchema);
  }

  @Override
  public void destroy() {
    // LOG.error("Calling destroy");
    executor.triggerShutdown();
  }

  /**
   * Builds the emitter's final output schema using the output field provided, along with the input fields.
   *
   * @param schema
   * @return output schema
   */
  private Schema buildOutputSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    // Since binary to be executed, can produce null the output for a particular input, hence creating nullable schema
    // for the output field
    Schema.Field field = Schema.Field.of(config.outputField, Schema.nullableOf(Schema.of(Schema.Type.valueOf(
      config.outputFieldType.trim().toUpperCase()))));
    if (fields.contains(field)) {
      throw new IllegalArgumentException(String.format("Field %s already has type specified. Duplicate field %s found.",
                                                       config.outputField, config.outputField));
    } else {
      fields.add(field);
    }
    return Schema.recordOf("output-schema", fields);
  }

  /**
   * Configuration class for Run.
   */
  public static class RunConfig extends PluginConfig {

    @Description("Command that will contain the full path to the executable binary present on the local filesystem of" +
      " the Hadoop nodes as well as how to execute that binary. For example, java -jar /home/user/ExampleRunner.jar, " +
      "if the binary to be executed is of type jar.")
    private final String commandToExecute;

    @Description("A comma-separated sequence of the fields to be used as variable command line arguments for binary " +
      "to be executed. For example, 'firstname' or 'firstname,lastname' in case of multiple arguments. Please make " +
      "sure that the sequence of fields/arguments is in the order as expected by binary. (Macro Enabled)")
    @Nullable
    @Macro
    private final String fieldsToProcess;

    @Description("A space-separated sequence of the fixed command line arguments that will be passed to the " +
      "executable binary. Please make sure that the sequence of arguments is in the order as expected by binary. All " +
      "the fixed command line arguments will be followed by the variable command line arguments, provided through " +
      "'Fields to Process for Variable Arguments'. (Macro enabled)")
    @Nullable
    @Macro
    private final String fixedArguments;

    @Description("The field name that holds the output of the executable binary.")
    private final String outputField;

    @Description("Schema type of the 'Output Field'. Supported types are: boolean, bytes, double, float, int, long " +
      "and string.")
    private final String outputFieldType;

    public RunConfig(String commandToExecute, @Nullable String fieldsToProcess, @Nullable String fixedArguments,
                     String outputField, String outputFieldType) {
      this.commandToExecute = commandToExecute;
      this.fieldsToProcess = fieldsToProcess;
      this.fixedArguments = fixedArguments;
      this.outputField = outputField;
      this.outputFieldType = outputFieldType;
    }

    /**
     * Validates whether the binary executable type is supported or not.
     */
    private void validateBinaryExecutableType() {
      String executableExtension = "";
      int separatorPosition = commandToExecute.lastIndexOf('.');
      if (separatorPosition > 0) {
        String extensionStrings[] = commandToExecute.substring(separatorPosition + 1).trim().split(" ");
        executableExtension = extensionStrings[0];
      } else {
        throw new IllegalArgumentException(
          String.format("Error while accessing the binary. Please make sure that the 'Command to Execute' is " +
                          "in the expected format. '%s'", commandToExecute));
      }
      switch (executableExtension) {
        case "jar":
        case "sh":
        case "exe":
        case "bat":
          break;
        default:
          throw new IllegalArgumentException(
            String.format("Binary type '%s' is not supported. Supported executable types are: 'exe, sh, jar and bat'" +
                            ".", executableExtension));
      }
    }

    /**
     * Validates whether the input field to process, is present in input schema or not.
     *
     * @param inputSchema
     */
    private void validateInputFields(Schema inputSchema) {
      if (!Strings.isNullOrEmpty(fieldsToProcess)) {
        for (String inputField : Splitter.on(',').trimResults().split(fieldsToProcess)) {
          if (inputSchema.getField(inputField) == null) {
            throw new IllegalArgumentException(
              String.format("Input field '%s' does not exist in the input schema: '%s'.", inputField, inputSchema));
          }
        }
      }
    }

    /**
     * Validates whether the output field type is supported or not.
     */
    private void validateOutputFieldType() {
      switch (outputFieldType) {
        case "boolean":
        case "bytes":
        case "double":
        case "float":
        case "int":
        case "long":
        case "string":
          break;
        default:
          throw new IllegalArgumentException(
            String.format("Schema type '%s' for output field is not supported. Supported types are: ' boolean, bytes," +
                            " double, float, int, long and string.", outputFieldType));
      }
    }
  }
}
