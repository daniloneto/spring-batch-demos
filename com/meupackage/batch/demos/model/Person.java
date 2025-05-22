package com.meupackage.batch.demos.model;

public class Person {
    private int id; // Added id field
    private String firstName;
    private String lastName;

    public Person() {
    }

    public Person(int id, String firstName, String lastName) { // Updated constructor
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public int getId() { // Getter for id
        return id;
    }

    public void setId(int id) { // Setter for id
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id + // Added id to toString
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                '}';
    }
}
