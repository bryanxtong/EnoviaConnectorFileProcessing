package com.bryan.fileprocessing;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * This class is used to delete files in a folder as Enovia connector generates
 * a lot of files , and remove files just to free up space for linux and "find"
 * command is taking more time to delete the same number of files
 * 
 * @author Tong Dayong
 *
 */
public class FilesDeletionRunner implements Runnable {

	private static Logger logger = Logger.getLogger(FilesDeletionRunner.class.getName());
	private String directory;

	// files removal numberOfDays ago
	private int numberOfDays;

	// make sure all the job start at the same time
	private LocalDateTime currentDateTime;

	public FilesDeletionRunner(String directory, LocalDateTime currentDateTime, int numberOfDays) {
		super();
		this.directory = directory;
		this.numberOfDays = numberOfDays;
		this.currentDateTime = currentDateTime;
	}

	/**
	 * delete all the files in a folder
	 */
	public void deleteAllFiles() {
		int count = 0;

		Path dir = FileSystems.getDefault().getPath(directory);
		try {
			DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.{xml}");
			for (Path path : stream) {
				logger.info(Thread.currentThread().getName() + " remove file: " + path.toString());
				count++;
				Files.deleteIfExists(path);
			}
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("delete " + count + " for folder " + directory);
	}

	/**
	 * delete the files number of days ago based on the current date time
	 * 
	 * @param currentDate
	 * @param numberOfDays
	 */
	public void deleteFiles1(LocalDateTime currentDate, int numberOfDays) {

		int count = 0;
		Path dir = FileSystems.getDefault().getPath(directory);
		LocalDateTime numberOfDaysAgo = currentDateTime.minusDays(numberOfDays);

		DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
			@Override
			public boolean accept(Path path) throws IOException {
				File file = path.toFile();
				if (!file.isDirectory() && file.getName().indexOf("xml") != -1) {
					LocalDateTime fileModifiedDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()),
							TimeZone.getDefault().toZoneId());
					if (fileModifiedDate.compareTo(numberOfDaysAgo) < 0) {
						return true;
					}
				}
				return false;
			}
		};

		try {
			DirectoryStream<Path> stream = Files.newDirectoryStream(dir, filter);
			for (Path path : stream) {
				logger.info(Thread.currentThread().getName() + " remove file: " + path.toString());
				count++;
				Files.deleteIfExists(path);
			}
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("delete " + count + " for folder " + directory);

	}

	/**
	 * delete files days ago based on the current time
	 */
	public void deleteFiles(LocalDateTime currentDateTime, int numberOfDays) {
		Path dir = FileSystems.getDefault().getPath(directory);
		LocalDateTime numberOfDaysAgo = currentDateTime.minusDays(numberOfDays);

		try {
			Stream<Path> stream = Files.find(dir, 1, (Path path, BasicFileAttributes basicFileAttrs) -> {
				File file = path.toFile();
				if (!file.isDirectory() && file.getName().indexOf("xml") != -1) {
					LocalDateTime fileModifiedDate = basicFileAttrs.lastModifiedTime().toInstant()
							.atZone(ZoneId.systemDefault()).toLocalDateTime();
					if (fileModifiedDate.compareTo(numberOfDaysAgo) < 0) {
						return true;
					}
				}
				return false;
			});

			stream.forEach((path) -> {
				logger.info(Thread.currentThread().getName() + " remove file: " + path.toString());
				try {
					Files.deleteIfExists(path);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});

			long numberOfFiles = stream.count();
			stream.close();

			logger.info("delete " + numberOfFiles + " for folder " + directory);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		if (numberOfDays == 0 || numberOfDays < 0) {
			logger.info("delete all files for folder" + directory);
			this.deleteAllFiles();
		} else {
			logger.info("delete files days ago: " + numberOfDays + " for folder " + directory);
			// delete files days ago based on the current time
			this.deleteFiles1(this.currentDateTime, numberOfDays);
		}
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();

		if (args.length != 1) {
			usage();
		}

		int numberOfDays = Integer.parseInt(args[0]);

		// indicate the start time for all jobs
		LocalDateTime localDateTime = LocalDateTime.now();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		// For Enovia connector MPN unmatched files
		FilesDeletionRunner mpnDeletionRunner = new FilesDeletionRunner("/data/csidata/EnoviaMPN_temp_Unmatch",
				localDateTime, numberOfDays);

		// For Enovia connector CP unmatched files
		FilesDeletionRunner cpDeletionRunner = new FilesDeletionRunner("/data/csidata/EnoviaCPtemp_Unmatch",
				localDateTime, numberOfDays);

		executorService.submit(cpDeletionRunner);
		executorService.submit(mpnDeletionRunner);

		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		long endTime = System.currentTimeMillis();
		logger.info("Finish deleting mpn and cp files and It takes " + (endTime - startTime) / 1000 + "s");

	}

	private static void usage() {
		System.err.println("Please use the following command: java FilesDeletionRunner \"<numberOfdays>\"");
		System.exit(-1);
	}
}
