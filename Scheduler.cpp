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
#include <set>
#include <cassert>
#include <climits>
#include <algorithm>

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

void StateChangeCompleteGreedy(Time_t time, MachineId_t machine_id);
void StateChangeCompletePMapper(Time_t time, MachineId_t machine_id);
void StateChangeCompleteEECO(Time_t time, MachineId_t machine_id);
void StateChangeCompleteResearch(Time_t time, MachineId_t machine_id);

void SLAWarningGreedy(Time_t time, TaskId_t task_id);
void SLAWarningPMapper(Time_t time, TaskId_t task_id);
void SLAWarningEECO(Time_t time, TaskId_t task_id);
void SLAWarningResearch(Time_t time, TaskId_t task_id);

void MemoryWarningGreedy(Time_t time, MachineId_t machine_id);
void MemoryWarningPMapper(Time_t time, MachineId_t machine_id);
void MemoryWarningEECO(Time_t time, MachineId_t machine_id);
void MemoryWarningResearch(Time_t time, MachineId_t machine_id);

// Static variables for Greedy algorithm state
static vector<TaskId_t> pending_tasks;
static vector<VMId_t> *p_vms = nullptr;
static vector<MachineId_t> *p_machines = nullptr;
struct PendingMigration
{
    VMId_t vm_id;
    MachineId_t source_machine;
    MachineId_t target_machine;
    unsigned memory_impact;
};
static std::vector<PendingMigration> pending_migrations;

// Helper functions
static Priority_t determine_priority(TaskId_t task_id)
{
    SLAType_t sla = RequiredSLA(task_id);
    switch (sla)
    {
    case SLA0:
        return HIGH_PRIORITY;
    case SLA1:
        return MID_PRIORITY;
    case SLA2:
    case SLA3:
        return LOW_PRIORITY;
    default:
        return LOW_PRIORITY;
    }
}
unsigned GetProjectedMemoryUsed(MachineId_t machine_id)
{
    MachineInfo_t machine_info = Machine_GetInfo(machine_id);
    unsigned projected_memory = machine_info.memory_used;

    for (const auto &migration : pending_migrations)
    {
        if (migration.target_machine == machine_id)
        {
            projected_memory += migration.memory_impact; // Incoming VM
        }
        else if (migration.source_machine == machine_id)
        {
            projected_memory -= migration.memory_impact; // Outgoing VM
        }
    }
    return projected_memory;
}

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
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 1);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    vms.clear();
    machines.clear();
    pending_tasks.clear();
    unsigned total_machines = Machine_GetTotal();
    machines.resize(total_machines);
    for (unsigned i = 0; i < machines.size(); i++)
    {
        machines[i] = MachineId_t(i);
    }

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

    // Set pointers to access vms and machines outside the class
    p_vms = &vms;
    p_machines = &machines;
}

void InitGreedy(vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::InitGreedy(): Initializing Greedy algorithm", 1);
    for (unsigned i = 0; i < machines.size(); i++)
    {
        Machine_SetState(machines[i], S5); // Start with all machines off
    }
    // wait until all machines are in S5
    bool allOff = false;
    while (!allOff)
    {
        allOff = true;
        for (unsigned i = 0; i < machines.size(); i++)
        {
            MachineInfo_t machine_info = Machine_GetInfo(machines[i]);
            if (machine_info.s_state != S5)
            {
                SimOutput("Scheduler::InitGreedy(): Machine " + to_string(machines[i]) + " is not off", 1);
                allOff = false;
                break;
            }
        }
    }

    SimOutput("Scheduler::InitGreedy(): All machines are off", 1);
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
    /*
    Steps:
    1) Find suitable VM, if found: attach
    2) If no VM, find suitable machine, if found: create VM, attach
    3) If no machine, turn on machine, create VM, attach
    */
    SimOutput("Scheduler::NewTaskGreedy(): Received new task " + to_string(task_id) + " at time " + to_string(now), 1);
    VMType_t required_vm_type = RequiredVMType(task_id);
    CPUType_t required_cpu_type = RequiredCPUType(task_id);
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_memory = GetTaskMemory(task_id);
    Priority_t priority = determine_priority(task_id);

    // Find suitable VM
    VMId_t suitable_vm = VMId_t(-1);
    unsigned min_remaining_memory = UINT_MAX;
    for (auto vm_id : vms)
    {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        // Check if the VM is compatible with the task
        if (vm_info.vm_type == required_vm_type &&
            vm_info.cpu == required_cpu_type &&
            Machine_GetInfo(vm_info.machine_id).gpus == gpu_capable)
        {
            // Check if machine has space for the task
            MachineInfo_t machine_info = Machine_GetInfo(vm_info.machine_id);
            unsigned total_load = machine_info.memory_used + task_memory;
            float u_plus_v = (float)total_load / machine_info.memory_size;
            if (machine_info.s_state == S0 && u_plus_v < 1.0f)
            {
                unsigned remaining = machine_info.memory_size - machine_info.memory_used;
                if (remaining < min_remaining_memory)
                { // Find machine with highest utilization
                    min_remaining_memory = remaining;
                    suitable_vm = vm_id;
                }
            }
        }
    }

    // If suitable VM found, add task to VM
    if (suitable_vm != VMId_t(-1))
    {
        VM_AddTask(suitable_vm, task_id, priority);
        SimOutput("Scheduler::NewTaskGreedy(): Task " + to_string(task_id) + " placed on VM " + to_string(suitable_vm), 1);
        return;
    }

    // No suitable VM found, now find suitable machine to create VM
    MachineId_t suitable_machine = MachineId_t(-1);
    min_remaining_memory = UINT_MAX;
    for (auto machine_id : machines)
    {
        // Check if machine can handle launching new VM and adding task
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        unsigned total_load = machine_info.memory_used + VM_MEMORY_OVERHEAD + task_memory;
        float u_plus_v = (float)total_load / machine_info.memory_size;
        if (machine_info.s_state == S0 && machine_info.cpu == required_cpu_type && u_plus_v < 1.0f)
        {
            unsigned remaining = machine_info.memory_size - machine_info.memory_used;
            if (remaining < min_remaining_memory)
            { // Find machine with highest utilization
                min_remaining_memory = remaining;
                suitable_machine = machine_id;
            }
        }
    }

    if (suitable_machine != MachineId_t(-1))
    {
        VMId_t new_vm = VM_Create(required_vm_type, required_cpu_type);
        VM_Attach(new_vm, suitable_machine);
        VM_AddTask(new_vm, task_id, priority);
        vms.push_back(new_vm);
        SimOutput("Scheduler::NewTaskGreedy(): Task " + to_string(task_id) + " placed on new VM " + to_string(new_vm) + " on machine " + to_string(suitable_machine), 1);
        return;
    }

    // No suitable VM or machine found; turn on new machine, change state to S0, then wait for StateChangeComplete to add task
    for (auto machine_id : machines)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S5 && machine_info.cpu == required_cpu_type)
        {
            Machine_SetState(machine_id, S0);
            pending_tasks.push_back(task_id);
            SimOutput("Scheduler::NewTaskGreedy(): Turning on machine " + to_string(machine_id) + " for task " + to_string(task_id), 1);
            return;
        }
    }

    ThrowException("Scheduler::NewTaskGreedy(): No machine available for task " + to_string(task_id) + ", SLA violation", 1);
}

void NewTaskPMapper(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskPMapper(): Received new task " + to_string(task_id) + " at time " + to_string(now), 1);
    // TODO
}

void NewTaskEECO(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskEECO(): Received new task " + to_string(task_id) + " at time " + to_string(now), 1);
    // TODO
}

void NewTaskResearch(Time_t now, TaskId_t task_id, vector<VMId_t> &vms, vector<MachineId_t> &machines)
{
    SimOutput("Scheduler::NewTaskResearch(): Received new task " + to_string(task_id) + " at time " + to_string(now), 1);
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
    SimOutput("Scheduler::TaskCompleteGreedy(): Task " + to_string(task_id) + " completed at time " + to_string(now), 1);

    // Sort machines by projected utilization
    vector<pair<MachineId_t, float>> machine_utils;
    for (auto machine_id : *p_machines)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S0)
        {
            unsigned projected_memory = GetProjectedMemoryUsed(machine_id);
            float u = (float)projected_memory / machine_info.memory_size;
            machine_utils.emplace_back(machine_id, u);
        }
    }
    sort(machine_utils.begin(), machine_utils.end(), [](const pair<MachineId_t, float> &a, const pair<MachineId_t, float> &b)
         { return a.second < b.second; });

    // Consolidate by migrating VMs from low-utilization machines
    for (size_t j = 0; j < machine_utils.size(); j++)
    {
        MachineId_t machine_id = machine_utils[j].first;
        float u = machine_utils[j].second;
        if (u == 0.0f)
            continue; // Skip empty machines

        // Skip machines with pending incoming migrations
        bool has_incoming = false;
        for (const auto &migration : pending_migrations)
        {
            if (migration.target_machine == machine_id)
            {
                has_incoming = true;
                break;
            }
        }
        if (has_incoming)
            continue;

        // Collect VMs not currently migrating
        vector<VMId_t> vms_to_migrate;
        for (auto vm_id : *p_vms)
        {
            VMInfo_t vm_info = VM_GetInfo(vm_id);
            if (vm_info.machine_id == machine_id)
            {
                bool is_migrating = false;
                for (const auto &migration : pending_migrations)
                {
                    if (migration.vm_id == vm_id)
                    {
                        is_migrating = true;
                        break;
                    }
                }
                if (!is_migrating)
                {
                    vms_to_migrate.push_back(vm_id);
                }
            }
        }

        for (auto vm_id : vms_to_migrate)
        {
            VMInfo_t vm_info = VM_GetInfo(vm_id);
            CPUType_t cpu_type = vm_info.cpu;
            bool gpu_capable = Machine_GetInfo(vm_info.machine_id).gpus;
            unsigned vm_memory = 0;
            for (auto tid : vm_info.active_tasks)
            {
                vm_memory += GetTaskMemory(tid);
            }
            vm_memory += VM_MEMORY_OVERHEAD;

            // Find a target machine with higher utilization
            for (size_t k = j + 1; k < machine_utils.size(); k++)
            {
                MachineId_t target_machine = machine_utils[k].first;
                MachineInfo_t target_info = Machine_GetInfo(target_machine);
                unsigned projected_memory = GetProjectedMemoryUsed(target_machine);
                unsigned total_load = projected_memory + vm_memory;
                float target_u_plus_v = (float)total_load / target_info.memory_size;
                if (target_info.s_state == S0 && target_info.cpu == cpu_type &&
                    target_info.gpus == gpu_capable && target_u_plus_v < 1.0f)
                {
                    // Initiate migration and track it
                    VM_Migrate(vm_id, target_machine);
                    pending_migrations.push_back({vm_id, machine_id, target_machine, vm_memory});
                    SimOutput("Scheduler::TaskCompleteGreedy(): Migrating VM " + to_string(vm_id) +
                                  " from machine " + to_string(machine_id) + " to " + to_string(target_machine),
                              1);
                    break; // Move to next VM
                }
            }
        }

        // Check if machine is now empty (considering pending migrations)
        unsigned projected_memory = GetProjectedMemoryUsed(machine_id);
        if (projected_memory == 0)
        {
            Machine_SetState(machine_id, S5);
            SimOutput("Scheduler::TaskCompleteGreedy(): Turning off machine " + to_string(machine_id), 1);
        }
    }
}

void TaskCompletePMapper(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompletePMapper(): Task " + to_string(task_id) + " completed at time " + to_string(now), 1);
    // TODO
}

void TaskCompleteEECO(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompleteEECO(): Task " + to_string(task_id) + " completed at time " + to_string(now), 1);
    // TODO
}

void TaskCompleteResearch(Time_t now, TaskId_t task_id)
{
    SimOutput("Scheduler::TaskCompleteResearch(): Task " + to_string(task_id) + " completed at time " + to_string(now), 1);
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
    SimOutput("Scheduler::MigrationCompleteGreedy(): Migration of VM " + to_string(vm_id) +
                  " completed at time " + to_string(time),
              1);
    // Remove the completed migration from the pending list
    for (auto it = pending_migrations.begin(); it != pending_migrations.end();)
    {
        if (it->vm_id == vm_id)
        {
            it = pending_migrations.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void MigrationCompletePMapper(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompletePMapper(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 1);
    // TODO
}

void MigrationCompleteEECO(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompleteEECO(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 1);
    // TODO
}

void MigrationCompleteResearch(Time_t time, VMId_t vm_id)
{
    SimOutput("Scheduler::MigrationCompleteResearch(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 1);
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
    SimOutput("Scheduler::PeriodicCheckGreedy(): SchedulerCheck() called at " + to_string(now), 3);

    for (auto machine_id : *p_machines)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S0 && machine_info.active_tasks == 0)
        {
            for (auto it = p_vms->begin(); it != p_vms->end();)
            {
                VMInfo_t vm_info = VM_GetInfo(*it);
                if (vm_info.machine_id == machine_id && vm_info.active_tasks.empty())
                {
                    VM_Shutdown(*it);
                    it = p_vms->erase(it);
                }
                else
                {
                    ++it;
                }
            }
            machine_info = Machine_GetInfo(machine_id); // Refresh info
            assert(machine_info.active_vms == 0);
            if (machine_info.active_vms == 0)
            {
                Machine_SetState(machine_id, S5);
                SimOutput("Scheduler::PeriodicCheckGreedy(): Turning off machine " + to_string(machine_id), 3);
            }
        }
    }
}

void PeriodicCheckPMapper(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckPMapper(): SchedulerCheck() called at " + to_string(now), 1);
    // TODO
}

void PeriodicCheckEECO(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckEECO(): SchedulerCheck() called at " + to_string(now), 1);
    // TODO
}

void PeriodicCheckResearch(Time_t now)
{
    SimOutput("Scheduler::PeriodicCheckResearch(): SchedulerCheck() called at " + to_string(now), 1);
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
    SimOutput("SimulationComplete(): Finished!", 1);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 1);
}

// ------------------------
// Public interface below
// ------------------------
static Scheduler scheduler;

void InitScheduler()
{
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id)
{
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id)
{
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id)
{
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        MemoryWarningGreedy(time, machine_id);
        break;
    case PMAPPER:
        MemoryWarningPMapper(time, machine_id);
        break;
    case EECO:
        MemoryWarningEECO(time, machine_id);
        break;
    case RESEARCH:
        MemoryWarningResearch(time, machine_id);
        break;
    }
}

void MemoryWarningGreedy(Time_t time, MachineId_t machine_id)
{
    // Assume memory warning on machine
    // Pick workload with highest utilization, and apply SLAWarningGreedy()
    SimOutput("MemoryWarning(): Memory warning on machine " + to_string(machine_id) + " at time " + to_string(time), 1);

    // Sort VMs by utilization
    vector<pair<VMId_t, float>> vm_utils;
    for (auto vm_id : *p_vms)
    {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        if (vm_info.machine_id == machine_id)
        {
            unsigned total_load = 0;
            for (auto tid : vm_info.active_tasks)
            {
                total_load += GetTaskMemory(tid);
            }
            total_load += VM_MEMORY_OVERHEAD;
            float u = (float)total_load / Machine_GetInfo(machine_id).memory_size;
            vm_utils.emplace_back(vm_id, u);
        }
    }
    sort(vm_utils.begin(), vm_utils.end(), [](const pair<VMId_t, float> &a, const pair<VMId_t, float> &b)
         {
             return a.second > b.second; // Sort descending
         });
    if (vm_utils.empty())
        return; // No VMs on this machine

    // Apply SLAWarningGreedy to the VM with highest utilization
    VMId_t vm_id = vm_utils[0].first;
    VMInfo_t vm_info = VM_GetInfo(vm_id);
    for (auto tid : vm_info.active_tasks)
    {
        SLAWarningGreedy(time, tid);
    }
    SimOutput("MemoryWarning(): Applied SLAWarningGreedy to VM " + to_string(vm_id) + " on machine " + to_string(machine_id), 1);
}

void MemoryWarningPMapper(Time_t time, MachineId_t machine_id)
{
}

void MemoryWarningEECO(Time_t time, MachineId_t machine_id)
{
}

void MemoryWarningResearch(Time_t time, MachineId_t machine_id)
{
}

void MigrationDone(Time_t time, VMId_t vm_id)
{
    // The function is called on to alert you that migration is complete
    scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time)
{
    // This function is called periodically by the simulator, no specific event
    scheduler.PeriodicCheck(time);
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
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 1);

    scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id)
{
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        SLAWarningGreedy(time, task_id);
        break;
    case PMAPPER:
        SLAWarningPMapper(time, task_id);
        break;
    case EECO:
        SLAWarningEECO(time, task_id);
        break;
    case RESEARCH:
        SLAWarningResearch(time, task_id);
        break;
    }
}

void SLAWarningGreedy(Time_t time, TaskId_t task_id)
{
    SimOutput("SLAWarning(): Task " + to_string(task_id) + " violated SLA at time " + to_string(time), 1);

    // Find the VM hosting the task
    VMId_t current_vm = VMId_t(-1);
    MachineId_t current_machine = MachineId_t(-1);
    for (auto vm_id : *p_vms)
    {
        VMInfo_t vm_info = VM_GetInfo(vm_id);
        if (find(vm_info.active_tasks.begin(), vm_info.active_tasks.end(), task_id) != vm_info.active_tasks.end())
        {
            current_vm = vm_id;
            current_machine = vm_info.machine_id;
            break;
        }
    }

    if (current_vm == VMId_t(-1))
        return; // Task not found

    VMInfo_t vm_info = VM_GetInfo(current_vm);
    unsigned task_memory = GetTaskMemory(task_id);
    CPUType_t cpu_type = vm_info.cpu;
    bool gpu_capable = Machine_GetInfo(vm_info.machine_id).gpus;

    // Sort machines by utilization
    vector<pair<MachineId_t, float>> machine_utils;
    for (auto machine_id : *p_machines)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S0 && machine_id != current_machine)
        {
            float u = (float)machine_info.memory_used / machine_info.memory_size;
            machine_utils.emplace_back(machine_id, u);
        }
    }
    sort(machine_utils.begin(), machine_utils.end(), [](const pair<MachineId_t, float> &a, const pair<MachineId_t, float> &b)
         { return a.second < b.second; });

    // Try migrating to another machine
    for (auto &[machine_id, u] : machine_utils)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        unsigned total_load = machine_info.memory_used + task_memory + VM_MEMORY_OVERHEAD;
        float u_plus_v = (float)total_load / machine_info.memory_size;
        if (machine_info.cpu == cpu_type && machine_info.gpus == gpu_capable && u_plus_v < 1.0f)
        {
            // Check to see if there's a VM already on this machine that can take the task
            for (auto vm_id : *p_vms)
            {
                VMInfo_t vm_info = VM_GetInfo(vm_id);
                if (vm_info.machine_id == machine_id && vm_info.cpu == cpu_type)
                {
                    VM_AddTask(vm_id, task_id, determine_priority(task_id));
                    VM_RemoveTask(current_vm, task_id);
                    SimOutput("SLAWarning(): Migrated task " + to_string(task_id) + " to existing VM " + to_string(vm_id) + " on machine " + to_string(machine_id), 1);
                    return;
                }
            }
            // If not, create a new VM
            VMId_t new_vm = VM_Create(vm_info.vm_type, cpu_type);
            VM_Attach(new_vm, machine_id);
            VM_AddTask(new_vm, task_id, determine_priority(task_id));
            VM_RemoveTask(current_vm, task_id);
            p_vms->push_back(new_vm);
            SimOutput("SLAWarning(): Migrated task " + to_string(task_id) + " to new VM " + to_string(new_vm) + " on machine " + to_string(machine_id), 1);
            return;
        }
    }

    // Try turning on a standby machine
    for (auto machine_id : *p_machines)
    {
        MachineInfo_t machine_info = Machine_GetInfo(machine_id);
        if (machine_info.s_state == S5 && machine_info.cpu == cpu_type)
        {
            Machine_SetState(machine_id, S0);
            pending_tasks.push_back(task_id);
            VM_RemoveTask(current_vm, task_id);
            SimOutput("SLAWarning(): Turning on machine " + to_string(machine_id) + " for task " + to_string(task_id), 1);
            return;
        }
    }

    ThrowException("SLAWarning(): Failed to resolve SLA violation for task " + to_string(task_id), 0);
}

void SLAWarningPMapper(Time_t time, TaskId_t task_id)
{
}

void SLAWarningEECO(Time_t time, TaskId_t task_id)
{
}

void SLAWarningResearch(Time_t time, TaskId_t task_id)
{
}

void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
    switch (CURRENT_ALGORITHM)
    {
    case GREEDY:
        StateChangeCompleteGreedy(time, machine_id);
        break;
    case PMAPPER:
        StateChangeCompletePMapper(time, machine_id);
        break;
    case EECO:
        StateChangeCompleteEECO(time, machine_id);
        break;
    case RESEARCH:
        StateChangeCompleteResearch(time, machine_id);
        break;
    }
}

void StateChangeCompleteGreedy(Time_t time, MachineId_t machine_id)
{
    MachineInfo_t machine_info = Machine_GetInfo(machine_id);
    SimOutput("StateChangeCompleteGreedy(): Machine " + to_string(machine_id) + " state changed to " + to_string(machine_info.s_state) + " at time " + to_string(time), 1);
    if (machine_info.s_state == S0)
    {
        // Place all pending tasks on a machine WITH matching properties
        vector<TaskId_t> placed_tasks;
        for (auto tid : pending_tasks)
        {
            VMType_t required_vm_type = RequiredVMType(tid);
            CPUType_t required_cpu_type = RequiredCPUType(tid);
            bool gpu_capable = IsTaskGPUCapable(tid);
            unsigned task_memory = GetTaskMemory(tid);
            Priority_t priority = determine_priority(tid);

            // Look for a suitable VM to place the task on ANY machine
            VMId_t best_vm = VMId_t(-1);
            unsigned min_remaining_memory = UINT_MAX;
            for (auto vm_id : *p_vms)
            {
                VMInfo_t vm_info = VM_GetInfo(vm_id);
                if (vm_info.vm_type == required_vm_type && vm_info.cpu == required_cpu_type && Machine_GetInfo(vm_info.machine_id).gpus == gpu_capable)
                {
                    MachineInfo_t minfo = Machine_GetInfo(vm_info.machine_id);
                    unsigned total_load = minfo.memory_used + task_memory;
                    float u_plus_v = (float)total_load / minfo.memory_size;
                    if (minfo.s_state == S0 && u_plus_v < 1.0f)
                    {
                        unsigned remaining = minfo.memory_size - minfo.memory_used;
                        if (remaining < min_remaining_memory)
                        {
                            min_remaining_memory = remaining;
                            best_vm = vm_id;
                        }
                    }
                }
            }

            // If a suitable VM is found, place the task on it
            if (best_vm != VMId_t(-1))
            {
                VM_AddTask(best_vm, tid, priority);
                placed_tasks.push_back(tid);
                SimOutput("StateChangeComplete(): Placed task " + to_string(tid) + " on VM " + to_string(best_vm), 1);
                continue;
            }

            // If no suitable VM is found, create a new VM on the machine we just turned on
            MachineInfo_t minfo = Machine_GetInfo(machine_id);
            unsigned total_load = minfo.memory_used + VM_MEMORY_OVERHEAD + task_memory;
            float u_plus_v = (float)total_load / minfo.memory_size;
            if (minfo.cpu == required_cpu_type && u_plus_v < 1.0f)
            {
                VMId_t new_vm = VM_Create(required_vm_type, required_cpu_type);
                VM_Attach(new_vm, machine_id);
                VM_AddTask(new_vm, tid, priority);
                p_vms->push_back(new_vm);
                placed_tasks.push_back(tid);
                SimOutput("StateChangeComplete(): Placed task " + to_string(tid) + " on new VM " + to_string(new_vm) + " on machine " + to_string(machine_id), 1);
            }
        }

        for (auto tid : placed_tasks)
        {
            pending_tasks.erase(remove(pending_tasks.begin(), pending_tasks.end(), tid), pending_tasks.end());
        }
    }
}

void StateChangeCompletePMapper(Time_t time, MachineId_t machine_id)
{
}

void StateChangeCompleteEECO(Time_t time, MachineId_t machine_id)
{
}

void StateChangeCompleteResearch(Time_t time, MachineId_t machine_id)
{
}