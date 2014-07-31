/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.sync;

import com.emc.atmos.sync.plugins.*;
import com.emc.atmos.sync.plugins.cas.CasDestination;
import com.emc.atmos.sync.plugins.cas.CasSource;
import com.emc.atmos.sync.util.TimingUtil;
import org.apache.commons.cli.*;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.util.Assert;

import java.io.File;
import java.util.*;

/**
 * New plugin-based sync program.  Can be configured in two ways:
 * 1) through a command-line parser
 * 2) through Spring.  Call run() on the AtmosSync2 object after your beans are
 * initialized.
 * @author cwikj
 */
public class AtmosSync2 implements Runnable, InitializingBean, DisposableBean {
	private static final Logger l4j = Logger.getLogger(AtmosSync2.class);
	private static final String ROOT_SPRING_BEAN = "sync";

	public static void main(String[] args) {
		Set<SyncPlugin> plugins = new HashSet<SyncPlugin>();
		
		plugins.add(new MetadataPlugin());
		plugins.add(new CommonOptions());
		plugins.add(new AtmosSource());
		plugins.add(new DummyDestination());
		plugins.add(new AtmosDestination());
		plugins.add(new StripAclPlugin());
		plugins.add(new IdLoggerPlugin());
		plugins.add(new DatabaseIdMapper());
		plugins.add(new FilesystemSource());
		plugins.add(new FilesystemDestination());
		plugins.add(new GladinetMapper());
		plugins.add(new S3Source());
		plugins.add(new RetryPlugin());
		plugins.add(new OverrideMimetypePlugin());
		plugins.add(new ShellCommandPlugin());
        plugins.add(new PolicyTransitionPlugin());
        plugins.add(new S3Destination());
        plugins.add(new LoggingPlugin());
        plugins.add(new CasSource());
        plugins.add(new CasDestination());
        try {
            plugins.add(new ArchiveFileSource());
        } catch (UnsupportedClassVersionError e) {
            System.err.println("Note: archive support requires Java 7 or higher");
        }
		
		Map<String,SyncPlugin> optionMap = new HashMap<String, SyncPlugin>();
		
		// Add the generic options
		Options options = new Options();
		
		// Merge the options
		for(SyncPlugin plugin : plugins) {
            for (Object o1 : plugin.getOptions().getOptions()) {
                Option o = (Option) o1;
                if (options.hasOption(o.getOpt())) {
                    System.err.println("The plugin " +
                            optionMap.get(o.getOpt()).getName() +
                            " already installed option " + o.getOpt());
                } else {
                    options.addOption(o);
                    optionMap.put(o.getOpt(), plugin);
                }
            }
		}
		
		GnuParser gnuParser = new  GnuParser();
		CommandLine line = null;
		try {
			line = gnuParser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			shortHelp();
			System.exit(2);
		}
		
		// Special check for help
		if(line.hasOption(CommonOptions.HELP_OPTION)) {
			longHelp(plugins);
			System.exit(0);
		}

        if (l4j.isDebugEnabled()) {
            for (Option option : line.getOptions()) {
                if (option.hasArg())
                    LogMF.debug(l4j, "Parsed option {0}: {1}", option.getLongOpt(), line.getOptionValue(option.getLongOpt()));
                else
                    LogMF.debug(l4j, "Parsed option {0}", option.getLongOpt());
            }
        }
		
		AtmosSync2 sync;
		// Special check for Spring configuration
		if(line.hasOption(CommonOptions.SPRING_CONFIG_OPTION)) {
			sync = springBootstrap(line.getOptionValue(CommonOptions.SPRING_CONFIG_OPTION));
		} else {
			// Let the plugins parse the options and decide whether they want to
			// be included.
			sync = new AtmosSync2();
			for(SyncPlugin plugin : plugins) {
				if(plugin.parseOptions(line)) {
					sync.addPlugin(plugin);
				}
			}
			
			// Quick check for no-args
			if(sync.getSource() == null) {
				System.err.println("Source must be specified");
				shortHelp();
				System.exit(1);
			}
			if(sync.getDestination() == null) {
				System.err.println("Destination must be specified");
				shortHelp();
				System.exit(1);
			}

            if (line.hasOption(CommonOptions.TIMINGS_OPTION)) sync.setTimingsEnabled(true);

            if (line.hasOption(CommonOptions.TIMING_WINDOW_OPTION)) {
                sync.setTimingWindow( Integer.parseInt( line.getOptionValue( CommonOptions.TIMING_WINDOW_OPTION ) ) );
            }

			// do the sanity check (Spring will do this too)
			sync.afterPropertiesSet();
		}
		
		try {
			sync.run();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		sync.printStats();

		try {
			sync.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.exit(0);
	}
	
	/**
	 * Prints the stats from the source.
	 */
	private void printStats() {
        TimingUtil.logTimings(source);
		source.printStats();
	}

	/**
	 * Initializes a Spring Application Context from the given file and
	 * bootstraps the AtmosSync2 object from there.
	 */
	private static AtmosSync2 springBootstrap(String pathToSpringXml) {
		File springXml = new File(pathToSpringXml);
		if(!springXml.exists()) {
			System.err.println("The Spring XML file: " + springXml + " does not exist");
			System.exit(1);
		}
		
		l4j.info("Loading configuration from Spring XML file: " + springXml);
		FileSystemXmlApplicationContext ctx = 
				new FileSystemXmlApplicationContext(pathToSpringXml);
		
		if(!ctx.containsBean(ROOT_SPRING_BEAN)) {
			System.err.println("Your Spring XML file: " + springXml + 
					" must contain one bean named '" + ROOT_SPRING_BEAN +
					"' that initializes an AtmosSync2 object");
			System.exit(1);
		}
		
		return ctx.getBean(ROOT_SPRING_BEAN, AtmosSync2.class);
	}

    private static void shortHelp() {
        System.out.println("    use --help for a detailed (quite long) list of options");
    }

	private static void longHelp(Set<SyncPlugin> plugins) {
		HelpFormatter fmt = new HelpFormatter();
		
		// Make sure we do CommonOptions first
		for(SyncPlugin plugin : plugins) {
			if(plugin instanceof CommonOptions) {
				fmt.printHelp( "java -jar AtmosSync2.jar -source {source-uri} -destination {destination-uri} [options ...]\nCommon Options:", plugin.getOptions());
			}
		}
		System.out.println("\nThe following plugins are also installed and can be configured with their own options:\n");
		
		// Do the rest
		for(SyncPlugin plugin : plugins) {
			if(!(plugin instanceof CommonOptions)) {
				fmt.printHelp(plugin.getName() + " (" + plugin.getClass().getName() + ")\n" + plugin.getDocumentation(), plugin.getOptions());
			}
		}
	}

	
	private SourcePlugin source;
	private DestinationPlugin destination;
	private List<SyncPlugin> pluginChain;
    private boolean timingsEnabled = false;
    private int timingWindow = 10000;
	
	public AtmosSync2() {
		this.pluginChain = new ArrayList<SyncPlugin>();
	}

	private void addPlugin(SyncPlugin plugin) {
		if(plugin instanceof SourcePlugin) {
			LogMF.info(l4j, "Source: {0}: {1}", plugin.getName(), plugin.getClass());
			setSource((SourcePlugin) plugin);
		} else if(plugin instanceof DestinationPlugin) {
			LogMF.info(l4j, "Destination: {0}: {1}", plugin.getName(), plugin.getClass());
			setDestination((DestinationPlugin) plugin);
		} else {
			LogMF.info(l4j, "Plugin: {0}: {1}", plugin.getName(), plugin.getClass());
			pluginChain.add(plugin);
		}
	}

	public SourcePlugin getSource() {
		return source;
	}

	/**
	 * Sets the source plugin.
	 */
	public void setSource(SourcePlugin source) {
		if(this.source != null) {
			throw new IllegalStateException(
					"A source plugin is already configured (" + 
							source.getName() +")");
		}
		this.source = source;
	}

	public DestinationPlugin getDestination() {
		return destination;
	}

	/**
	 * Sets the destination plugin.
	 */
	public void setDestination(DestinationPlugin destination) {
		if(this.destination != null) {
			throw new IllegalStateException(
					"A destination plugin is already configured (" + 
							destination.getName() +")");
		}
		this.destination = destination;
	}

	public List<SyncPlugin> getPluginChain() {
		return pluginChain;
	}

	/**
	 * Sets the chain of plugins to insert between the source and destination.
	 * This is used for Spring configuration.  Don't put the source and
	 * destination in the chain; the afterPropertiesSet() method will do this
	 * for you.
	 * @param pluginChain a list of plugins to execute in between the source
	 * and destination.
	 */
	public void setPluginChain(List<SyncPlugin> pluginChain) {
		this.pluginChain = pluginChain;
	}

    public boolean isTimingsEnabled() {
        return timingsEnabled;
    }

    /**
     * When set to true, enables operation timings on all plug-ins that support it. Default is false.
     */
    public void setTimingsEnabled(boolean timingsEnabled) {
        this.timingsEnabled = timingsEnabled;
    }

    public int getTimingWindow() {
        return timingWindow;
    }

    /**
     * Sets the window for timing statistics. Every {timingWindow} objects that are synced, timing statistics are logged
     * and reset. Default is 10,000 objects.
     */
    public void setTimingWindow( int timingWindow ) {
        this.timingWindow = timingWindow;
    }

    /**
	 * Executes the current plugin chain and prints statistics when complete.
	 */
	@Override
	public void run() {
		source.run();
	}
	
	private void cleanup() {
		// Invoke cleanup on the plugins
		SyncPlugin p = source;
		while(p != null) {
			try {
				p.cleanup();
			} catch(Exception e) {
				LogMF.warn(l4j, "Failed to cleanup plugin {0}: {1}", p.getName(), e.getMessage());
			}
			p = p.getNext();
		}
	}

	@Override
	public void afterPropertiesSet() {
		// Some validation (must have source and destination)
		Assert.notNull(source, "Source plugin must be specified");
		Assert.notNull(destination, "Destination plugin must be specified");
		
		// Build the plugin chain
		SyncPlugin child = destination;
		for(int i=pluginChain.size()-1; i>=0; i--) {
			SyncPlugin current = pluginChain.get(i);
			current.setNext(child);
			child = current;
		}
		source.setNext(child);
		
		// Ask each plugin to validate the chain (resolves incompatible plugins)
		SyncPlugin p = source;
		while(p != null) {
			p.validateChain(source);
			p = p.getNext();
		}

        // register for timings
        if (timingsEnabled) TimingUtil.register(this, timingWindow);
	}

	@Override
	public void destroy() throws Exception {
		cleanup();
	}
}
