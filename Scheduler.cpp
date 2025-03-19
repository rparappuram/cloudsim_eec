//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include "Interfaces.h"
#include "SimTypes.h"
#include <map>

enum Algorithm
{
    GREEDY,
    PMAPPER,
    EECO,
    RESEARCH
}; // TODO: Placeholder - We need to research which algo we want to do
const Algorithm CURRENT_ALGORITHM = GREEDY;

// Free-standing function declarations
void InitGreedy(vector<VMId_t> &vms, vector<MachineId_t> &machines);
void InitPMapper(vector<VMId_t> &vms, vector<MachineId_t> &machines);
void InitEECO(vector<VMId_t> &vms, vector<MachineId_t> &machines);
void InitResearch(vector<VMId_t> &vms, vector<MachineId_t> &machines);

void NewTaskGreedy(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines);
void NewTaskPMapper(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines);
void NewTaskEECO(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines);
void NewTaskResearch(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines);

void TaskCompleteGreedy(Time_t now, TaskId_t task_id);
void TaskCompletePMapper(Time_t now, TaskId_t task_id);
void TaskCompleteEECO(Time_t now, TaskId_t task_id);
void TaskCompleteResearch(Time_t now, TaskId_t task_id);

void PeriodicCheckGreedy(Time_t now);
void PeriodicCheckPMapper(Time_t now);
void PeriodicCheckEECO(Time_t now);
void PeriodicCheckResearch(Time_t now);

void MigrationCompleteGreedy(Time_t time, VMId_t vm_id);
void MigrationCompletePMapper(Time_t time, VMId_t vm_id);
void MigrationCompleteEECO(Time_t time, VMId_t vm_id);
void MigrationCompleteResearch(Time_t time, VMId_t vm_id);

static bool migrating = false;
static unsigned active_machines = 16;                                                 // TODO: Maybe not needed?
static std::map<MachineId_t, bool> waking_machines;                                   // Tracks machines transitioning to S0
static std::map<MachineId_t, std::vector<std::pair<TaskId_t, VMType_t>>> task_queues; // Queues tasks waiting for each machine

void Scheduler::Init()
{
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    //
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    unsigned total_machines = Machine_GetTotal();
    machines.resize(total_machines);
    for (unsigned i = 0; i < machines.size(); i++)
        machines[i] = MachineId_t(i);

    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        InitGreedy(vms, machines);
        break;
    case PMAPPER:
        InitPMapper(vms, machines);
        break;
    case EECO:
        InitEECO(vms, machines);
        break;
    case RESEARCH:
        InitResearch(vms, machines);
        break;
    }
}

void InitGreedy(vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::InitGreedy(): Initializing Greedy algorithm", 1);
    // VMs created on demand in NewTaskGreedy
}

void InitPMapper(vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::InitPMapper(): Initializing PMapper algorithm", 1);
    // TODO
}

void InitEECO(vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::InitEECO(): Initializing EECO algorithm", 1);
    // TODO
}

void InitResearch(vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::InitResearch(): Initializing Research algorithm", 1);
    // TODO
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id)
{
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM,
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        NewTaskGreedy(now, task_id, vms, machines);
        break;
    case PMAPPER:
        NewTaskPMapper(now, task_id, vms, machines);
        break;
    case EECO:
        NewTaskEECO(now, task_id, vms, machines);
        break;
    case RESEARCH:
        NewTaskResearch(now, task_id, vms, machines);
        break;
    }
}

void NewTaskGreedy(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskGreedy(): Received new task " + to_string(task_id) + " at time " + to_string(now), 4);
}

void NewTaskPMapper(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskPMapper(): Received new task " + to_string(task_id) + " at time " + to_string(now), 4);
    // TODO
}

void NewTaskEECO(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskEECO(): Received new task " + to_string(task_id) + " at time " + to_string(now), 4);
    // TODO
}

void NewTaskResearch(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskResearch(): Received new task " + to_string(task_id) + " at time " + to_string(now), 4);
    // TODO
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id)
{
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        TaskCompleteGreedy(now, task_id);
        break;
    case PMAPPER:
        TaskCompletePMapper(now, task_id);
        break;
    case EECO:
        TaskCompleteEECO(now, task_id);
        break;
    case RESEARCH:
        TaskCompleteResearch(now, task_id);
        break;
    }
}

void TaskCompleteGreedy(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompleteGreedy(): Task " + to_string(task_id) + " completed at time " + to_string(now), 4);
    // TODO
}

void TaskCompletePMapper(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompletePMapper(): Task " + to_string(task_id) + " completed at time " + to_string(now), 4);
    // TODO
}

void TaskCompleteEECO(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompleteEECO(): Task " + to_string(task_id) + " completed at time " + to_string(now), 4);
    // TODO
}

void TaskCompleteResearch(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompleteResearch(): Task " + to_string(task_id) + " completed at time " + to_string(now), 4);
    // TODO
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id)
{
    // Update your data structure. The VM now can receive new tasks
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        MigrationCompleteGreedy(time, vm_id);
        break;
    case PMAPPER:
        MigrationCompletePMapper(time, vm_id);
        break;
    case EECO:
        MigrationCompleteEECO(time, vm_id);
        break;
    case RESEARCH:
        MigrationCompleteResearch(time, vm_id);
        break;
    }
}

void MigrationCompleteGreedy(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompleteGreedy(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
    // TODO
}

void MigrationCompletePMapper(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompletePMapper(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
    // TODO
}

void MigrationCompleteEECO(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompleteEECO(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
    // TODO
}

void MigrationCompleteResearch(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompleteResearch(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
    // TODO
}

void Scheduler::PeriodicCheck(Time_t now)
{
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        PeriodicCheckGreedy(now);
        break;
    case PMAPPER:
        PeriodicCheckPMapper(now);
        break;
    case EECO:
        PeriodicCheckEECO(now);
        break;
    case RESEARCH:
        PeriodicCheckResearch(now);
        break;
    }
}

void PeriodicCheckGreedy(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckGreedy(): SchedulerCheck() called at " + to_string(now), 4);
    // TODO
}

void PeriodicCheckPMapper(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckPMapper(): SchedulerCheck() called at " + to_string(now), 4);
    // TODO
}

void PeriodicCheckEECO(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckEECO(): SchedulerCheck() called at " + to_string(now), 4);
    // TODO
}

void PeriodicCheckResearch(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckResearch(): SchedulerCheck() called at " + to_string(now), 4);
    // TODO
}

void Scheduler::Shutdown(Time_t time)
{
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for (auto &vm : vms)
    {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

// ------------------------
// Public interface below
// ------------------------
static Scheduler scheduler;

void InitScheduler()
{
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id)
{
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id)
{
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id)
{
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
    // TODO: Figure out what to do
}

void MigrationDone(Time_t time, VMId_t vm_id)
{
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time)
{
    // This function is called periodically by the simulator, no specific event
    // TODO: modify, maybe?
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    scheduler.PeriodicCheck(time);
    static unsigned counts = 0;
    counts++;
    if (counts == 10)
    {
        migrating = true;
        VM_Migrate(1, 9);
    }
}

void SimulationComplete(Time_t time)
{
    // This function is called before the simulation terminates. TODO: Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl; // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);

    scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id)
{
    // TODO:
}

void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
    // TODO: Figure out what to do
}
