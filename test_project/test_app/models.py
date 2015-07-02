from django.db import models

class Person(models.Model):
    name = models.CharField(max_length=120)
    email = models.EmailField()

    def __unicode__(self):
        return self.name

    class Meta:
        abstract = True

class Reader(Person):
    pass

class Author(Person):
    pass

class Book(models.Model):
    title = models.CharField(max_length=120)
    author = models.ForeignKey(Author)
    isbn = models.CharField(max_length=120)
    read_by = models.ManyToManyField(Reader)

    def __unicode__(self):
        return self.title

