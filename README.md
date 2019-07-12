# pulsar-flink
Elastic data processing with Apache Pulsar and Apache Flink



### Checkstyle For Java
IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository.
2. Configure the plugin by going to Settings -> Other Settings -> Checkstyle.
3. Set the "Scan Scope" to "Only Java sources (including tests)".
4. Select _8.14_ in the "Checkstyle Version" dropdown and click apply. **This step is important,
   don't skip it!**
5. In the "Configuration File" pane, add a new configuration using the plus icon:
    1. Set the "Description" to "Flink".
    2. Select "Use a local Checkstyle file", and point it to
      `"tools/maven/checkstyle.xml"` within
      your repository.
    3. Check the box for "Store relative to project location", and click
      "Next".
    4. Configure the "checkstyle.suppressions.file" property value to
      `"suppressions.xml"`, and click "Next", then "Finish".
6. Select "Flink" as the only active configuration file, and click "Apply" and
   "OK".
7. Checkstyle will now give warnings in the editor for any Checkstyle
   violations.

Once the plugin is installed you can directly import `"tools/maven/checkstyle.xml"` by going to Settings -> Editor -> Code Style -> Java -> Gear Icon next to Scheme dropbox. This will for example automatically adjust the imports layout.

You can scan an entire module by opening the Checkstyle tools window and
clicking the "Check Module" button. The scan should report no errors.