package com.bryan.fileprocessing;


import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enovia Connector generates a lot of xml files in temp folder. This class is
 * used to identify whether a file modified time is between a start time and end
 * time and also whether the modified time in xml is also between this time
 * interval
 *
 * @author tongda
 *
 */
public class FilesFilterRunner {

	// <select name="modified">2/11/2018 2:53:51 AM</select>
	private static final String REGEX_EXPRESSION = "<select name=\"modified\">(.+?)</select>";
	private static final boolean MOVE_FILE_OR_NOT = false;
	private static final String FILTERED_FILE_TYPE = "xml";

	// modified date in xml files
	private static final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a";

	/**
	 * process the file content as string
	 *
	 * @param f
	 * @return
	 * @throws IOException
	 */
	public String readFileAsString(File f) throws IOException {
		return new String(Files.readAllBytes(Paths.get(f.getParent(), f.getName())));

	}

	/**
	 * whether the modified time in xml file match the time interval [startTime ->
	 * endTime]
	 *
	 * @param xmlMessage
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws IOException
	 */
	public boolean match(File f, Date startDate, Date endDate) {

		String content = null;
		try {
			content = this.readFileAsString(f);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		List<Date> dateList = new ArrayList<>();
		Pattern pattern = Pattern.compile(REGEX_EXPRESSION);
		final Matcher matcher = pattern.matcher(content);
		DateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

		try {
			while (matcher.find()) {
				dateList.add(sdf.parse(matcher.group(1)));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		for (Date date : dateList) {
			if (date.compareTo(startDate) > 0 && date.compareTo(endDate) <= 0) {
				return true;
			}
		}

		return false;
	}

	/**
	 * find all the files between startDate and endDate
	 *
	 * @param startDate
	 * @param endDate
	 */
	public void processFiles(String sourceFolder, String destFolder, Date startDate, Date endDate) {

		int count = 0;
		Path dir = FileSystems.getDefault().getPath(sourceFolder);
		DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
			@Override
			public boolean accept(Path path) throws IOException {
				File file = path.toFile();
				if (!file.isDirectory() && file.getName().indexOf(FILTERED_FILE_TYPE) != -1) {
					long modifiedDate = file.lastModified();
					Date fileModifiedDate = new Date(modifiedDate);
					if (fileModifiedDate.compareTo(startDate) > 0 && fileModifiedDate.compareTo(endDate) <= 0) {
						return true;
					}
				}
				return false;
			}
		};
		try {
			DirectoryStream<Path> stream = Files.newDirectoryStream(dir, filter);
			for (Path p : stream) {
				String fileName = p.getFileName().toFile().getName();
				if (MOVE_FILE_OR_NOT) {
					count ++;
					Files.move(Paths.get(sourceFolder, fileName), Paths.get(destFolder, fileName),
							StandardCopyOption.REPLACE_EXISTING);
				}
				System.out.println(fileName);
			}
			System.out.println("process count " + count);
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		String strStartDate = "2018-03-02 00:00:00";
		String strEndDate = "2018-03-28 16:07:30";
		final String sourceFolder = "/var/user/csidev/Autonomy10/EnoviaConnector/EnoviaMP2013x/temp";
		final String destFolder = "/var/user/csidev/Bryan/tempjava";

		Date startDate = null;
		Date endDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			startDate = sdf.parse(strStartDate);
			endDate = sdf.parse(strEndDate);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println(
				"processing files from " + strStartDate + " to " + strEndDate + " at time " + sdf.format(new Date()));
		new FilesFilterRunner().processFiles(sourceFolder, destFolder, startDate, endDate);
		System.out.println(new Date());
	}

}
