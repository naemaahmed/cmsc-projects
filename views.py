from django.shortcuts import render, get_object_or_404
from django.http import Http404
from django.http import HttpResponse, HttpResponseRedirect
from grading.models import Instructor, Course, Assignment, Student, StudentAssignment, Question
from django.urls import reverse
import datetime
from django.utils import timezone

# Create your views here.
def mainindex(request):
        context = { 'instructor_list': Instructor.objects.all() }
        return render(request, 'grading/index.html', context)

def instructorindex(request, instructor_id):
    try:
        i = Instructor.objects.get(pk=instructor_id)
        submissions = []
        c_list = i.course_set.all()
        for c in i.course_set.all():
            submissions.append(c.title + ": " + str(StudentAssignment.objects.filter(assignment__course_id__title=c.title).count()) + " submissions")
    except Instructor.DoesNotExist:
        raise Http404("Instructor does not exist")
    return render(request, 'grading/instructorindex.html', {'instructor_id': instructor_id, 'course_list': i.course_set.all(), 'submissions': submissions})

def instructorcourse(request, instructor_id, course_id):
	c = get_object_or_404(Course, pk=course_id)
	a_list = c.assignment_set.filter(due_date__gte=timezone.now())
	p_list = c.assignment_set.filter(due_date__lte=timezone.now())
	context = {'name': Instructor.objects.get(pk=instructor_id), 'instructor_id': instructor_id, 'course_id': course_id, 'course_title': c.title, 'active_assignment_list': a_list, 'past_assignment_list': p_list }
        return render(request, 'grading/instructorcourse.html', context)

# Should get a list of all submissions for this assignment, and set it in context
def instructorassignment(request, instructor_id, course_id, assignment_id):
    course = Course.objects.get(pk=course_id)
    students = Student.objects.filter(courses__pk=course_id, courses__instructor_id=instructor_id)
    #check if assignment is past or active by retrieving due date
    x = Assignment.objects.filter(pk=assignment_id, course_id=course_id, course__instructor_id=instructor_id)
    if x:
        empty = False
        temp = x[0].due_date
        #past assignment
        if temp < timezone.now():
            past = True
        #active assignment
        else:
            past = False
    else:
        empty = True
        past = True
    #seperate students with submissions and students without submissions
    students_with_submissions = []
    students_no_submissions = []
    submissions = StudentAssignment.objects.filter(assignment_id=assignment_id, assignment__course_id=course_id)
    for s in students:
        for x in submissions:
            if s.pk is x.student_id:
                students_with_submissions.append(s)
    flag = True
    for s in students_with_submissions:
        for x in students:
            if s is x:
                flag = False
        if flag:
            students_no_submissions.append(s)
        flag = True
    i = Instructor.objects.get(pk=instructor_id)
    context = {'i': i, 'course_id': course_id, 'empty': empty, 'past': past, 'students_with_submissions': students_with_submissions, 'course': course,'students': students, 'assignment_id': assignment_id, 'submissions': submissions}
    return render(request, 'grading/instructorassignment.html', context)

def instructorcreate(request, instructor_id, course_id):
        context = { 'course_list': Instructor.objects.all() }
        return render(request, 'grading/instructorcreate.html', context)

def instructorgradesubmission(request, instructor_id, course_id, assignment_id, student_id):
    submission = StudentAssignment.objects.filter(assignment_id=assignment_id, student_id=student_id, assignment__course_id=course_id)
    correct_ans = Question.objects.filter(assignment_id=assignment_id,assignment__course_id=course_id)
    s_answer = submission[0].answers
    context = {'correct_ans': correct_ans,'s_answer': s_answer,'a_id':assignment_id, 'i_id':instructor_id, 'c_id': course_id,'s_id': Student.objects.get(pk=student_id), 'submission': submission}
    return render(request, 'grading/instructorgradesubmission.html', context)

def studentindex(request, student_id):
    assignments = Assignment.objects.filter(course__student__id=student_id)
    a_list = assignments.filter(due_date__gte=timezone.now())
    p_list = assignments.filter(due_date__lte=timezone.now())
    context = { 'p_list': p_list,'a_list': a_list,'student_id': student_id, 'course_list': Student.objects.get(pk=student_id).courses.all(), 'name': Student.objects.get(pk=student_id) }
    return render(request, 'grading/studentindex.html', context)

def studentassignment(request, student_id, assignment_id):
	context = { 'assignment': Assignment.objects.get(pk=assignment_id), 'student': Student.objects.get(pk=student_id) }
        return render(request, 'grading/studentassignment.html', context)

def submitassignment(request, student_id, assignment_id):
	print request.POST
	answers = " ".join([request.POST["answer{}".format(i)] for i in range(1, 101) if "answer{}".format(i) in request.POST])
	sa = StudentAssignment(student=Student.objects.get(pk=student_id), assignment=Assignment.objects.get(pk=assignment_id), answers=answers, score=-1)
	sa.save()
	return HttpResponseRedirect(reverse('studentindex', args=(student_id)))

def submittedassignment(request, student_id, assignment_id):
    context = { 'student_id': student_id, 'course_list': Student.objects.get(pk=student_id).courses.all() }
    return render(request, 'grading/studentindex.html', context)

#############################################################################

def gradeassignment(request, instructor_id, course_id, assignment_id, student_id):
    print request.POST
    sa = StudentAssignment.objects.get(student_id=student_id, assignment_id=assignment_id, assignment__course_id=course_id)
    sa.score = request.POST
    sa.save()
    return HttpResponseRedirect(reverse('instructorassignment', args=(instructor_id, course_id, assignment_id)))

















