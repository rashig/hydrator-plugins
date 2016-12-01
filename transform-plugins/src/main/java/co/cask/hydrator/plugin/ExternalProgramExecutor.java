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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A package private class for executing an external program.
 */
final class ExternalProgramExecutor extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalProgramExecutor.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final String name;
  private final String executable;
  private final String[] args;
  private final BlockingQueue<Event> eventQueue;
  // private List<ListenableFuture<String>> completions = Lists.newLinkedList();
  private ExecutorService executor;
  private Process process;
  private Thread shutdownThread;
  private List<String> outputList = new ArrayList<>();
  private List<String> errorList = new ArrayList<>();

  ExternalProgramExecutor(String name, String executable, String... args) {
    this.name = name;
    this.executable = executable;
    this.args = args;
    this.eventQueue = new LinkedBlockingQueue<Event>();
  }

  /**
   * Creates a new {@link ExternalProgramExecutor} using settings from another one.
   *
   * @param other {@link ExternalProgramExecutor} to clone from.
   */
  ExternalProgramExecutor(ExternalProgramExecutor other) {
    this.name = other.name;
    this.executable = other.executable;
    this.args = other.args;
    this.eventQueue = new LinkedBlockingQueue<Event>();
  }

  void submit(String line, SettableFuture<String> completion, Emitter<StructuredRecord> emitter,
              StructuredRecord structuredRecord, Schema outputSchema) {
    System.out.println("calling submit");
    List<ListenableFuture<String>> completions = Lists.newLinkedList();
    // Preconditions.checkState(isRunning(), "External program {} is not running.", this);
    try {
      eventQueue.put(new Event(line, completion));
      completions.add(completion);
      Futures.successfulAsList(completions).get();

      //process the output
      for (String output : outputList) {
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputSchema.getFields()) {
          if (structuredRecord.getSchema().getField(field.getName()) != null) {
            builder.set(field.getName(), structuredRecord.get(field.getName()));
          } else {
            if (field.getSchema().getType().equals(Schema.Type.STRING)) {
              builder.set(field.getName(), output);
            } else {
              builder.convertAndSet(field.getName(), output);
            }
          }
        }
        emitter.emit(builder.build());
        outputList.clear();
      }

      /*for (String error : errorList) {
        LOG.error(error);
        emitter.emitError(new InvalidEntry<StructuredRecord>(31, error, structuredRecord));
        errorList.clear();
      }*/

    } catch (Exception e) {
      completion.setException(e);
    } finally {
      System.out.println("End of submit");
    }

  }

  @Override
  protected String getServiceName() {
    return super.getServiceName() + "-" + name;
  }

  @Override
  protected void startUp() throws Exception {
    // We need two threads.
    // One thread for keep reading from input, write to process stdout and read from stdin.
    // The other for keep reading stderr and log.
    executor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("process-" + name + "-%d").build());

    // the Shutdown thread is to time the shutdown and kill the process if it timeout.
    shutdownThread = createShutdownThread();

    process = Runtime.getRuntime().exec(executable);

    executor.execute(createProcessRunnable(process));
    executor.execute(createLogRunnable(process));
  }

  @Override
  protected void run() throws Exception {
    // Simply wait for the process to complete.
    // Trigger shutdown would trigger closing of in/out streams of the process,
    // which if the process implemented correctly, should complete.
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      LOG.error("Process {} exit with exit code {}", this, exitCode);
    }
  }

  @Override
  protected void triggerShutdown() {
    executor.shutdownNow();
    shutdownThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    shutdownThread.interrupt();
    shutdownThread.join();

    // Need to notify all pending events as failure.
    List<Event> events = Lists.newLinkedList();
    eventQueue.drainTo(events);

    for (Event event : events) {
      event.getCompletion().setException(
        new IllegalStateException("External program " + toString() + " already stopped."));
    }
  }

  private Thread createShutdownThread() {
    Thread t = new Thread("shutdown-" + name) {
      @Override
      public void run() {
        // Wait for at most SHUTDOWN_TIME and kill the process
        try {
          TimeUnit.SECONDS.sleep(SHUTDOWN_TIMEOUT_SECONDS);
          LOG.warn("Process {} took too long to stop. Killing it.", this);
          process.destroy();
        } catch (InterruptedException e) {
          // If interrupted, meaning the process has been shutdown nicely.
        }
      }
    };
    t.setDaemon(true);
    return t;
  }

  private Runnable createProcessRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        System.out.println("Entered process runnable");
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.UTF_8));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(process.getOutputStream(), Charsets.UTF_8), true);

        try {
          while (!Thread.currentThread().isInterrupted()) {
            Event event = eventQueue.take();
            try {
              writer.println(event.getLine());
              String line = reader.readLine();
              outputList.add(line);
              event.getCompletion().set(line);
            } catch (IOException e) {
              LOG.error("Exception when sending events to {}. Event: {}.",
                        ExternalProgramExecutor.this, event.getLine());
              break;
            }
          }
        } catch (InterruptedException e) {
          // No-op. It's just for signal stopping of the thread.
        } finally {
          Closeables.closeQuietly(writer);
          Closeables.closeQuietly(reader);
          System.out.println("Exit process runnable");
        }
      }
    };
  }

  private Runnable createLogRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        System.out.println("Entered log runnable");
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), Charsets.UTF_8));
        try {
          String line = reader.readLine();
          System.out.println("Error is " + line);
          while (!Thread.currentThread().isInterrupted() && line != null) {
            System.out.println("Entered in log loop");
            LOG.info(line);
            errorList.add(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          LOG.error("Exception when reading from stderr stream for {}.", ExternalProgramExecutor.this);
        } finally {
          Closeables.closeQuietly(reader);
          System.out.println("Exit log runnable");
        }
      }
    };
  }

  private static final class Event {
    private final String line;
    private final SettableFuture<String> completion;

    Event(String line, SettableFuture<String> completion) {
      this.line = line;
      this.completion = completion;
    }

    private String getLine() {
      return line;
    }

    private SettableFuture<String> getCompletion() {
      return completion;
    }
  }
}
