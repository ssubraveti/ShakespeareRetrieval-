package com.invertedindex;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class ParseJSON {
    JSONArray allDocuments;


    ParseJSON(String filename) {

        try {
            JSONParser parser = new JSONParser();
            Object parse = parser.parse(new FileReader(filename));
            JSONObject parsedOutput = (JSONObject) parse;
            allDocuments = (JSONArray) parsedOutput.get("corpus");
        } catch (FileNotFoundException fe) {
            System.out.print("File not found");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.print("I/O exception");
            ioe.printStackTrace();
        } catch (ParseException pe) {
            System.out.print("Parsing error");
            pe.printStackTrace();
        }
    }

    JSONArray getAllDocuments() {
        return allDocuments;
    }
}
