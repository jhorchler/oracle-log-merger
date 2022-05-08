import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Olm {

    public static final int BATCH_SIZE = 1024;

    public static void main(String[] args)
    throws ParserConfigurationException, SAXException, IOException {

        Olm merger = new Olm();
        merger.go(args);

    }

    private void go(String[] cliargs)
    throws ParserConfigurationException, SAXException, IOException {

        // vars needed
        String h2DbFile = new String();
        String sourceFile = new String();
        String destinationFile = new String();

        // add options
        Option dbFile = Option.builder("db")
                              .longOpt("database")
                              .desc("set the name of the H2 database to use")
                              .hasArg()
                              .numberOfArgs(1)
                              .build();
        Option loadXml = Option.builder("l")
                               .longOpt("load")
                               .desc("load a given XML file")
                               .hasArg()
                               .numberOfArgs(1)
                               .build();
        Option dumpXml = Option.builder("p")
                               .longOpt("print")
                               .desc("dump the contents of the database")
                               .hasArg()
                               .numberOfArgs(1)
                               .build();
        Option help = Option.builder("h")
                            .longOpt("help")
                            .desc("print this message")
                            .build();
        OptionGroup operation = new OptionGroup();
        operation.addOption(loadXml);
        operation.addOption(dumpXml);

        // parse options
        Options options = new Options();
        options.addOption(help);
        options.addOption(dbFile);
        options.addOptionGroup(operation);
        HelpFormatter helpFormat = new HelpFormatter();
        CommandLineParser cliparser = new DefaultParser();

        try {

            CommandLine line = cliparser.parse(options, cliargs);

            // need help?
            if (line.hasOption("help") || line.hasOption("h")) {

                helpFormat.printHelp("oxlm", options);
                System.exit(0);

            }

            // check if DB option was given
            if (line.hasOption("db") || line.hasOption("database")) {

                h2DbFile = line.getOptionValue("db");

            } else {

                throw new ParseException("Need to operate on a DB file");

            }

            // check optionGroup: must be given
            String selectedOperation = operation.getSelected();
            if (selectedOperation == null) {

                throw new ParseException(
                    "Either dump the DB or load an xml file");

            } else if (selectedOperation.equals("load") ||
                       selectedOperation.equals("l")) {

                sourceFile = line.getOptionValue("load");

            } else if (selectedOperation.equals("print") ||
                       selectedOperation.equals("p")) {

                destinationFile = line.getOptionValue("print");

            }

        } catch (ParseException e) {

            System.err.println(String.format("Parsing failed. Reason: %s",
                                             e.getMessage()));
            helpFormat.printHelp("oxlm", options);
            System.exit(1);

        }

        if (sourceFile.isEmpty()) { // dump database

            dumpDatabase(destinationFile, h2DbFile);

        } else { // load XML file

            loadXml(sourceFile, h2DbFile);

        }

    }

    private void loadXml(String xmlFile, String dbFile)
    throws ParserConfigurationException, SAXException {

        /*
         * construct input stream for sax parser
         * Oracle Corp. decided to use invalid xml logfiles. They
         * do not contain a single XML root element that nests all
         * others. Instead the document contains one <msg></msg> after
         * another. So instead of using a File object a FileInputStream
         * (bytestream) is used to feed the SAX parser. This stream is
         * pre- and appended with <root> ... messages ... </root>.
         */
        File file = new File(xmlFile);

        try (FileInputStream fis = new FileInputStream(file)) {

            List<InputStream> streams = Arrays.asList(
                new ByteArrayInputStream("<root>".getBytes()), fis,
                new ByteArrayInputStream("</root>".getBytes()));

            try (InputStream cntr = new SequenceInputStream(
                    Collections.enumeration(streams))) {

                // get sax parser
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();

                // create custom handler and parse the document
                OxlmHandler oxlmHandler = new OxlmHandler(dbFile);
                saxParser.parse(cntr, oxlmHandler);

            } catch (IOException ioe) {

                System.err.println(
                    String.format("Creating input stream failed. Reason: %s",
                                  ioe.getMessage()));
                System.exit(1);

            }

        } catch (IOException ioe) {

            System.err.println(
                String.format("Could not open xml File. Reason: %s",
                              ioe.getMessage()));
            System.exit(1);

        }

    }

    private void dumpDatabase(String destinationFile, String dbFile) {

        // get connection
        String dbUrl = String.format(
            "jdbc:h2:file:%s;CACHE_SIZE=65536;ACCESS_MODE_DATA=r", dbFile);

        try (Connection h2Connection =
                DriverManager.getConnection(dbUrl, "sa", "");
             Statement selectStatement = h2Connection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
             )) {

            selectStatement.setFetchSize(BATCH_SIZE);
            ResultSet messages = selectStatement.executeQuery("""
                SELECT id
                     , time
                     , component
                     , host
                     , message
                  FROM messages
                 ORDER BY time asc;
            """);

            // write the resultset to the wanted CSV file
            try (CSVPrinter outCsvFile = new CSVPrinter(
                    new FileWriter(destinationFile),
                    CSVFormat.RFC4180
                        .withHeader("Time", "Component", "Host", "Message")
                        .withDelimiter('|')
                        .withAutoFlush(true)
                        .withEscape('"'))) {

                while (messages.next()) {

                    outCsvFile.printRecord(
                        messages.getObject(2, OffsetDateTime.class),
                        messages.getString(3),
                        messages.getString(4),
                        messages.getString(5)
                    );

                }

            } catch(IOException ie) {

                System.err.println(
                    String.format("Write failed. Reason: %s", ie.getMessage()));
                System.exit(1);

            }

            messages.close();

        } catch (SQLException sqe) {

            System.err.println(String.format("Select failed. Reason: %s",
                                             sqe.getMessage()));
            System.exit(1);

        }

    }

    private class OxlmHandler
    extends DefaultHandler {

        private OracleAlertMessageDto message;
        private StringBuffer alertText;
        private String dbFileName;
        private Connection h2Connection;
        private String dbUrl = "jdbc:h2:file:%s;" +
                               "LOG=0;" + // disable the transaction log
                               "CACHE_SIZE=65536;" + // memory cache
                               "LOCK_MODE=0;" + // disable locking
                               "UNDO_LOG=0;" + // disable undo log
                               "MAX_OPERATION_MEMORY=10485760"; // 100MB for inserts/delete
        private PreparedStatement insertStatement;
        private int currentRow = 0;

        // Set database file name here. No validation. Just set the string.
        public OxlmHandler(String dbFile) {

            this.dbFileName = dbFile;

            try {

                // get DB connection
                this.h2Connection =
                    DriverManager.getConnection(String.format(this.dbUrl,
                                                              this.dbFileName),
                                                "sa", "");
                this.h2Connection.setAutoCommit(false);

                // create messages table
                try (Statement createStatement =
                        this.h2Connection.createStatement()) {

                    createStatement.executeUpdate("""
                        CREATE TABLE IF NOT EXISTS messages (
                            id          UUID PRIMARY KEY,
                            time        TIMESTAMP WITH TIME ZONE,
                            component   VARCHAR2(100),
                            host        VARCHAR2(256),
                            message     VARCHAR2(4000)
                        )
                    """);
                    this.h2Connection.commit();

                }

                // prepare the INSERT statement
                String insertSQL = """
                    INSERT INTO messages (id, time, component, host, message)
                    VALUES (?, ?, ?, ?, ?)
                """;
                this.insertStatement = this.h2Connection.prepareStatement(insertSQL);

            } catch (SQLException e) {

                System.err.println(String.format(
                    "Init of H2DB failed. Reason: %s", e.getMessage()));
                System.exit(1);

            }
            // use a single alertMessage DTO
            this.message = new OracleAlertMessageDto();
            this.alertText = new StringBuffer();

        }

        /*
         * endDocument is called once when EOF is found.
         * It inserts the last objects that are contained in the
         * insertStement-batch and closes the DB connection.
         */
        @Override
        public void endDocument()
        throws SAXException {

            // holds a int with 1 or 0 for every statement in the batch
            // showing whether the statement was executed successfully
            int[] updateCount;

            // execute the batch and close the statement & connection
            try {

                updateCount = this.insertStatement.executeBatch();
                this.h2Connection.commit();
                System.out.println(String.format("Inserted %d rows",
                                   IntStream.of(updateCount).sum()));
                this.insertStatement.close();
                this.h2Connection.close();

            } catch (SQLException e) {

                System.err.println(
                    String.format(
                        "Closing connection or final insert failed. Reason: %s",
                        e.getMessage()));

            }

        }

        /*
         * startElement is called on every <> element found.
         * It is used to increment the row count for <msg> tags found and
         * extracts from the attributes of this tag
         * - comp_id
         * - host_id
         * - time
         * and additionally it generates a random UUID as primary key.
         * These attributes are set on the singleton DTO used.
         * Finally it clears the existing message text of the last <txt> found.
         */
        @Override
        public void startElement(String uri, String localName, String qName,
                                 Attributes attributes)
        throws SAXException {

            // if this is a msg extract attributes from it
            switch (qName) {

                case "msg":
                    currentRow++;
                    this.message.setMessageID(UUID.randomUUID());
                    this.message.setComponentID(attributes.getValue("comp_id"));
                    this.message.setHostID(attributes.getValue("host_id"));
                    this.message.setAlertTime(OffsetDateTime.parse(
                        attributes.getValue("time"),
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    ));

                    if (this.alertText.length() > 0) {

                        this.alertText.delete(0, this.alertText.length());
                        this.alertText.trimToSize();

                    }

                    break;

            }

        }

        /*
         * endElement is executed on every closing </> tag found.
         * If a </msg> is found it is used to
         * - add a single INSERT statement to the batch
         * - if the batch holds BATCH_SIZE statements already this batch is
         *   executed.
         * If a </txt> is found the alert message is completed and the
         * StringBuffer holding all fragements of the text is transfered to
         * the DTO used.
         */
        @Override
        public void endElement(String uri, String localName, String qName)
        throws SAXException {

            int[] updateCount;

            switch(qName) {

                case "msg":

                    try {

                        // add message to the batch
                        this.insertStatement.setObject(1,
                            this.message.getMessageID());
                        this.insertStatement.setObject(2,
                            this.message.getAlertTime(),
                            java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                        this.insertStatement.setString(3,
                            this.message.getComponentID());
                        this.insertStatement.setString(4,
                            this.message.getHostID());
                        this.insertStatement.setString(5,
                            this.message.getAlertMessage());
                        this.insertStatement.addBatch();

                        // execute the batch and reset the counter to 0
                        if (currentRow == BATCH_SIZE) {

                            updateCount = insertStatement.executeBatch();
                            this.h2Connection.commit();
                            System.out.println(
                                String.format("Inserted %d rows",
                                              IntStream.of(updateCount).sum()));
                            currentRow = 0;

                        }

                    } catch (Exception e) {

                        System.err.println(
                            String.format("Insert into DB failed. Reason: %s",
                            e.getMessage()));

                    }

                    break;

                case "txt":
                    this.message.setAlertMessage(
                        this.alertText.toString().strip());
                    break;

            }

        }

        /*
         * characters is executed if text is found inside <txt>...</txt>.
         * The character array holding a buffer of characters (may not the
         * whole text inside <txt></txt>) is appended to the StringBuffer
         * holding the message.
         */
        @Override
        public void characters(char[] ch, int start, int length)
        throws SAXException {

            this.alertText.append(ch, start, length);

        }

    }

    // just a plain Java bean .. nothing special
    private class OracleAlertMessageDto {

        private UUID messageID;
        private String componentID;
        private String hostID;
        private String alertMessage;
        private OffsetDateTime alertTime;

        public void setMessageID(UUID messageID) {

            this.messageID = messageID;

        }

        public UUID getMessageID() {

            return messageID;

        }

        public void setAlertMessage(String alertMessage) {

            this.alertMessage = alertMessage;

        }

        public String getAlertMessage() {

            return alertMessage;

        }

        public void setHostID(String hostID) {

            this.hostID = hostID;

        }

        public String getHostID() {

            return hostID;

        }

        public void setComponentID(String componentID) {

            this.componentID = componentID;

        }

        public String getComponentID() {

            return componentID;

        }

        public void setAlertTime(OffsetDateTime alertTime) {

            this.alertTime = alertTime;

        }

        public OffsetDateTime getAlertTime() {

            return alertTime;

        }

        @Override
        public String toString() {

            return String.format("%s|%s|%s|%s|%s", messageID, alertTime,
                                 componentID, hostID, alertMessage);

        }
    }

}
