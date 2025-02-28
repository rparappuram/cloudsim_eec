machine class:
{
# High performance with GPUs
    Number of machines: 8
    CPU type: X86
    Number of cores: 16
    Memory: 32768
    S-States: [200, 150, 120, 90, 50, 20, 0]
    P-States: [15, 10, 7, 4]
    C-States: [15, 5, 2, 0]
    MIPS: [2000, 1600, 1200, 800]
    GPUs: yes
}

machine class:
{
# ARM without GPUs
    Number of machines: 6
    CPU type: ARM
    Number of cores: 8
    Memory: 16384
    S-States: [100, 80, 60, 40, 20, 10, 0]
    P-States: [10, 7, 5, 3]
    C-States: [10, 4, 1, 0]
    MIPS: [1200, 900, 700, 500]
    GPUs: no
}

task class:
{
# High SLA, high GPU, AI tasks
    Start time: 50000
    End time: 1000000
    Inter arrival: 5000
    Expected runtime: 4000000
    Memory: 16
    VM type: LINUX
    GPU enabled: yes
    SLA type: SLA0
    CPU type: X86
    Task type: AI
    Seed: 987654
}

task class:
{
# Web workloads, moderate SLA
    Start time: 70000
    End time: 1200000
    Inter arrival: 10000
    Expected runtime: 1500000
    Memory: 4
    VM type: LINUX
    GPU enabled: no
    SLA type: SLA1
    CPU type: ARM
    Task type: WEB
    Seed: 123456
}

task class:
{
# Low-priority (background), crypto tasks
    Start time: 100000
    End time: 1400000
    Inter arrival: 20000
    Expected runtime: 8000000
    Memory: 12
    VM type: LINUX_RT
    GPU enabled: yes
    SLA type: SLA3
    CPU type: X86
    Task type: CRYPTO
    Seed: 789012
}